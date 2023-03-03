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
package io.trino.plugin.jdbc;

import io.trino.spi.type.TimestampType;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class TestStandardColumnMappings {
    @Test
    public void testToTrinoTimestampForStarRocks() {
        long epochMicros = StandardColumnMappings.toTrinoTimestampForStarRocks(TimestampType.TIMESTAMP_SECONDS,
                LocalDateTime.of(2019, 7, 26, 16, 37, 49, 0)
        );
        assertEquals(1564159069000000L, epochMicros);
        Instant instant = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(epochMicros));
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        assertEquals("2019-07-26T16:37:49", localDateTime.format(DateTimeFormatter.ISO_DATE_TIME));
    }
}
