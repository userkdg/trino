/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mysql.cj.jdbc.JdbcStatement;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.*;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.mysql.cj.exceptions.MysqlErrorNumbers.SQL_STATE_ER_TABLE_EXISTS_ERROR;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.*;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class StarRocksClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(StarRocksClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    // StarRocks(MySQL) driver returns width of timestamp types instead of precision.
    // 19 characters are used for zero-precision timestamps while others
    // require 19 + precision + 1 characters with the additional character
    // required for the decimal separator.
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    // StarRocks(MySQL) driver returns width of time types instead of precision, same as the above timestamp type.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;

    // An empty character means that the table doesn't have a comment in MySQL
    private static final String NO_COMMENT = "";
    private final Type jsonType;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;

    @Inject
    public StarRocksClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.statisticsEnabled = statisticsConfig.isEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(StarRocksClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (schemaName.equalsIgnoreCase("mysql")
                || schemaName.equalsIgnoreCase("sys")) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the StarRocks(MySQL) driver
            // attempts to drain the connection by reading all the results.
            connection.abort(directExecutor());
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // starrocks fix bug [https://github.com/StarRocks/starrocks/issues/18700]
        log.debug("Reading starRocks tables ...");
        return getStarRocksTables(connection, schemaName, tableName);
    }

    private ResultSet getStarRocksTables(Connection connection, Optional<String> schemaName,
                                        Optional<String> tableName) throws SQLException
    {
        String sql = "SELECT" +
                "  TABLE_SCHEMA TABLE_CAT," +
                "  NULL TABLE_SCHEM," +
                "  TABLE_NAME," +
                "  IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE)  TABLE_TYPE," +
                "  TABLE_COMMENT REMARKS," +
                "  NULL TYPE_CAT," +
                "  NULL TYPE_SCHEM," +
                "  NULL TYPE_NAME," +
                "  NULL SELF_REFERENCING_COL_NAME," +
                "  NULL REF_GENERATION " +
                "  FROM " +
                "  INFORMATION_SCHEMA.TABLES " +
                "  WHERE 1=1 " +
                (schemaName.isPresent() ? String.format(" AND TABLE_SCHEMA = '%s' ", schemaName.get()) : "") +
                (tableName.isPresent() ? String.format(" AND TABLE_NAME = '%s' ", tableName.get()) : "");
        log.debug("starRocks metadata sql=%s", sql);
        return connection.createStatement().executeQuery(sql);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in MySQL
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT = %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                mysqlVarcharLiteral(comment.orElse(NO_COMMENT))); // An empty character removes the existing comment in MySQL
        execute(session, sql);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // StarRocks(MySQL) uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting createTableSql ");
    }

    private static String mysqlVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "tinyint unsigned":
                return Optional.of(smallintColumnMapping());
            case "smallint unsigned":
                return Optional.of(integerColumnMapping());
            case "int unsigned":
                return Optional.of(bigintColumnMapping());
            case "bigint unsigned":
                return Optional.of(decimalColumnMapping(createDecimalType(20)));
            case "json":
                return Optional.of(jsonColumnMapping());
            case "enum":
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                // Disable pushdown because floating-point values are approximate and not stored as exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.getRequiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = Math.min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                // TODO does StarRocks(MySQL) support negative scale?
                precision = precision + Math.max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, Math.max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunctionUsingLocalDate(),
                        mySqlDateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.getRequiredColumnSize()));
                requireNonNull(timeType, "timeType is null");
                checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        mySqlTimeReadFunction(timeType),
                        timeWriteFunction(timeType.getPrecision())));

            case Types.TIMESTAMP:
                TimestampType timestampType = createTimestampType(getTimestampPrecision(typeHandle.getRequiredColumnSize()));
                checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
                return Optional.of(ColumnMapping.longMapping(
                        timestampType,
                        mySqlTimestampReadFunction(timestampType),
                        timestampWriteFunction(timestampType)));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private LongWriteFunction mySqlDateWriteFunctionUsingLocalDate()
    {
        return new LongWriteFunction() {
            @Override
            public String getBindExpression()
            {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException
            {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    private static LongReadFunction mySqlTimestampReadFunction(TimestampType timestampType)
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIMESTAMP type returns java.sql.Timestamp, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, LocalDateTime.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return timestampReadFunctionForStarRocks(timestampType).readLong(resultSet, columnIndex);
            }
        };
    }

    private static LongReadFunction mySqlTimeReadFunction(TimeType timeType)
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIME type returns java.sql.Time, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, String.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return timeReadFunction(timeType).readLong(resultSet, columnIndex);
            }
        };
    }

    private static int getTimestampPrecision(int timestampColumnSize)
    {
        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
        verify(1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected timestamp precision %s calculated from timestamp column size %s", timestampPrecision, timestampColumnSize);
        return timestampPrecision;
    }

    private static int getTimePrecision(int timeColumnSize)
    {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("int", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        // todo 支持starRocks LARGEINT https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-types/LARGEINT#largeint
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", mySqlDateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("datetime(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(format("datetime(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), longTimestampWriteFunction(timestampType, MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar(1048576)";
            } else {
                dataType = "varchar(" + varcharType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }
        if (type instanceof ArrayType arrayType){
            // todo 写入支持array
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + arrayType);
        }
        if (type instanceof HyperLogLogType){
            return WriteMapping.sliceMapping("hll hll_union", varcharWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = SQL_STATE_ER_TABLE_EXISTS_ERROR.equals(e.getSQLState());
            throw new TrinoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName) {
        // https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-definition/ALTER%20TABLE#alter-table 注意列名不支持修改。
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support rename column types");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT#create-table-as-select
        // https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-definition/CREATE%20TABLE%20AS%20SELECT#create-table-as-select
        String tableCopyFormat = "CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1";
        if (isGtidMode(connection)) {
            tableCopyFormat = "CREATE TABLE %s LIKE %s";
        }
        String sql = format(
                tableCopyFormat,
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // StarRocks(MySQL) doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        verify(remoteTableName.getSchemaName().isEmpty());
        renameTable(session, null, remoteTableName.getCatalogName().orElse(null), remoteTableName.getTableName(), newTableName);
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName) throws SQLException
    {
        // https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-definition/ALTER%20TABLE#%E4%BF%AE%E6%94%B9%E8%A1%A8%E5%90%8D
        execute(session, connection, format(
                "ALTER TABLE %s RENAME %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(catalogName, newRemoteSchemaName, newRemoteTableName)));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        // https://docs.starrocks.io/zh-cn/latest/sql-reference/sql-statements/data-manipulation/SELECT#order-by
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.getColumn().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        if (joinCondition.getOperator() == JoinCondition.Operator.IS_DISTINCT_FROM) {
            // Not supported in MySQL
            return false;
        }

        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        log.debug("Reading statistics for %s", table);
        try (Connection connection = connectionFactory.openConnection(session);
             Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            // TODO: starRocks当前版本为2.3，只能用抽样采集表_statistics_.table_statistic_v1
            //  2.5+可以用全量采集表_statistics_.column_statistics
            // https://docs.starrocks.io/zh-cn/latest/using_starrocks/Cost_based_optimizer#%E9%87%87%E9%9B%86%E7%B1%BB%E5%9E%8B
            long rowCount = 0;
            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            ImmutableMap<String, TableStatisticV1> tableStatisticV1Map = statisticsDao.getTableStatisticV1(table);
            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder();
                String columnName = column.getColumnName();
                TableStatisticV1 tableStatisticV1 = tableStatisticV1Map.get(columnName);
                if (tableStatisticV1 != null) {
                    rowCount = Math.max(rowCount, tableStatisticV1.rowCount());
                    columnStatisticsBuilder.setDataSize(Estimate.of(tableStatisticV1.dataSize()));
                    columnStatisticsBuilder.setDistinctValuesCount(Estimate.of(tableStatisticV1.distinctCount()));
                    columnStatisticsBuilder.setNullsFraction(Estimate.of(rowCount <= 0 ? 0 : (tableStatisticV1.nullCount() * 1.0 / rowCount)));
                    // varcharType will miss.
                    columnStatisticsBuilder.setRange(DoubleRange.from(column.getColumnType(), tableStatisticV1.min(), tableStatisticV1.max()));
                }
                tableStatistics.setColumnStatistics(column, columnStatisticsBuilder.build());
            }
            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static boolean isGtidMode(Connection connection)
    {
        try (java.sql.Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW VARIABLES LIKE 'gtid_mode'")) {
            if (resultSet.next()) {
                return !resultSet.getString("Value").equalsIgnoreCase("OFF");
            }

            return false;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private record StatisticsDao(Handle handle) {
        private StatisticsDao(Handle handle) {
            this.handle = requireNonNull(handle, "handle is null");
        }

        public ImmutableMap<String, TableStatisticV1> getTableStatisticV1(JdbcTableHandle table) {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            log.debug("remoteTableName=%s", remoteTableName.toString());
            return handle.createQuery("select " +
                            " db_name," +
                            " table_name," +
                            " column_name," +
                            " row_count," +
                            " data_size," +
                            " distinct_count," +
                            " null_count," +
                            " min," +
                            " max " +
                            " from _statistics_.table_statistic_v1 " +
                            " where table_name = :db_table_name ")
                    .bind("db_table_name", String.format("%s.%s", remoteTableName.getCatalogName().orElse(""), remoteTableName.getTableName()))
                    .map((rs, ctx) -> new TableStatisticV1(
                            rs.getString("db_name"),
                            rs.getString("table_name"),
                            rs.getString("column_name"),
                            rs.getLong("row_count"),
                            rs.getLong("data_size"),
                            rs.getLong("distinct_count"),
                            rs.getLong("null_count"),
                            rs.getString("min"),
                            rs.getString("max")))
                    .collect(toImmutableMap(TableStatisticV1::columnName, t -> t));
        }
    }

    /**
     * @param dbName default_cluster:ads_ka
     * @param tableName ads_ka.dim_day
     * @param distinctCount ndv
     */
    private record TableStatisticV1( String dbName,String tableName,String columnName, long rowCount, long dataSize,
                                    long distinctCount, long nullCount, String min, String max) {
        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("dbName", dbName())
                    .add("tableName", tableName())
                    .add("columnName", columnName())
                    .add("rowCount", rowCount())
                    .add("dataSize", dataSize())
                    .add("distinctCount", distinctCount())
                    .add("nullCount", nullCount())
                    .add("min", min())
                    .add("max", max())
                    .toString();
        }
    }

}
