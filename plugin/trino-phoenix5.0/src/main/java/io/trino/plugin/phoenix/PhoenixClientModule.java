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
package io.trino.plugin.phoenix;

import com.google.inject.*;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.*;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifierModule;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;

import javax.annotation.PreDestroy;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;
import static io.trino.plugin.phoenix.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.phoenix.PhoenixClient.DEFAULT_DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.phoenix.PhoenixErrorCode.PHOENIX_CONFIG_ERROR;
import static java.util.Objects.requireNonNull;

public class PhoenixClientModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public PhoenixClientModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new RemoteQueryModifierModule());
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForJdbcDynamicFiltering.class).to(PhoenixSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(ClassLoaderSafeConnectorRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(ClassLoaderSafeConnectorPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(QueryBuilder.class).to(DefaultQueryBuilder.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class));
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setDomainCompactionThreshold(DEFAULT_DOMAIN_COMPACTION_THRESHOLD));

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        bindSessionPropertiesProvider(binder, TypeHandlingJdbcSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcMetadataSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcWriteSessionProperties.class);
        bindSessionPropertiesProvider(binder, PhoenixSessionProperties.class);
        bindSessionPropertiesProvider(binder, JdbcDynamicFilteringSessionProperties.class);

        binder.bind(DynamicFilteringStats.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(JdbcMetadataConfig.class);
        configBinder(binder).bindConfig(JdbcWriteConfig.class);
        configBinder(binder).bindConfig(JdbcDynamicFilteringConfig.class);

        binder.bind(PhoenixClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(Key.get(PhoenixClient.class)).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(Key.get(JdbcClient.class, StatsCollecting.class)).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).annotatedWith(ForClassLoaderSafe.class).to(PhoenixMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(ClassLoaderSafeConnectorMetadata.class).in(Scopes.SINGLETON);

        binder.bind(ConnectionFactory.class)
                .annotatedWith(ForLazyConnectionFactory.class)
                .to(Key.get(ConnectionFactory.class, StatsCollecting.class))
                .in(Scopes.SINGLETON);
        install(conditionalModule(
                PhoenixConfig.class,
                PhoenixConfig::isReuseConnection,
                new ReusableConnectionFactoryModule(),
                innerBinder -> innerBinder.bind(ConnectionFactory.class).to(LazyConnectionFactory.class).in(Scopes.SINGLETON)));

        bindTablePropertiesProvider(binder, PhoenixTableProperties.class);
        binder.bind(PhoenixColumnProperties.class).in(Scopes.SINGLETON);

        binder.bind(PhoenixConnector.class).in(Scopes.SINGLETON);

        checkConfiguration(buildConfigObject(PhoenixConfig.class).getConnectionUrl());

        install(new JdbcDiagnosticModule());
        install(new IdentifierMappingModule());
        install(new DecimalModule());
    }

    private void checkConfiguration(String connectionUrl)
    {
        try {
            PhoenixDriver driver = PhoenixDriver.INSTANCE;
            checkArgument(driver.acceptsURL(connectionUrl), "Invalid JDBC URL for Phoenix connector");
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_CONFIG_ERROR, e);
        }
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(PhoenixConfig config)
            throws SQLException
    {
        return new ConfiguringConnectionFactory(
                new DriverConnectionFactory(
                        PhoenixDriver.INSTANCE, // Note: for some reason new PhoenixDriver won't work.
                        config.getConnectionUrl(),
                        getConnectionProperties(config),
                        new EmptyCredentialProvider()),
                connection -> {
                    // Per JDBC spec, a Driver is expected to have new connections in auto-commit mode.
                    // This seems not to be true for PhoenixDriver, so we need to be explicit here.
                    connection.setAutoCommit(true);
                });
    }

    public static Properties getConnectionProperties(PhoenixConfig config)
            throws SQLException
    {
        Configuration resourcesConfig = readConfig(config);
        Properties connectionProperties = new Properties();
        for (Map.Entry<String, String> entry : resourcesConfig) {
            connectionProperties.setProperty(entry.getKey(), entry.getValue());
        }

        PhoenixEmbeddedDriver.ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(config.getConnectionUrl());
        connectionInfo.asProps().asMap().forEach(connectionProperties::setProperty);
        return connectionProperties;
    }

    private static Configuration readConfig(PhoenixConfig config)
    {
        Configuration result = newEmptyConfiguration();
        for (String resourcePath : config.getResourceConfigFiles()) {
            result.addResource(new Path(resourcePath));
        }
        return result;
    }

    @Singleton
    @ForRecordCursor
    @Provides
    public ExecutorService createRecordCursorExecutor()
    {
        return newDirectExecutorService();
    }

    @PreDestroy
    public void shutdownRecordCursorExecutor(@ForRecordCursor ExecutorService executor)
    {
        executor.shutdownNow();
    }
}
