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
package com.facebook.presto.plugin.tiledb;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;

public final class TileDBSessionProperties
{
    private static final String READ_BUFFER_SIZE = "read_buffer_size";
    private static final String WRITE_BUFFER_SIZE = "write_buffer_size";
    private static final String AWS_ACCESS_KEY_ID = "aws_access_key_id";
    private static final String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";
    private static final String SPLITS = "splits";
    private static final String SPLIT_ONLY_PREDICATES = "split_only_predicates";
    private static final String ENABLE_STATS = "enable_stats";
    private static final String TILEDB_CONFIG = "tiledb_config";
    private static final String ENCRYPTION_KEY = "encryption_key";
    private static final String TIMESTAMP = "timestamp";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TileDBSessionProperties(TileDBConfig tileDBConfig, TileDBClient client)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        READ_BUFFER_SIZE,
                        "Size in bytes to use for read buffers",
                        tileDBConfig.getReadBufferSize(),
                        false),
                integerProperty(
                        WRITE_BUFFER_SIZE,
                        "Size in bytes to use for read buffers",
                        tileDBConfig.getWriteBufferSize(),
                        false),
                stringProperty(
                        AWS_ACCESS_KEY_ID,
                        "AWS Access Key ID used for s3 access",
                        tileDBConfig.getAwsAccessKeyId(),
                        false),
                stringProperty(
                        AWS_SECRET_ACCESS_KEY,
                        "AWS Secret Access Key used for s3 access",
                        tileDBConfig.getAwsSecretAccessKey(),
                        false),
                integerProperty(
                        SPLITS,
                        "Maximum number of splits to perform, this is best attempt. Connector will always split on dimension ranges, even if this is more than configured max splits. -1 value means split to match number of worker nodes",
                        -1,
                        false),
                booleanProperty(
                        SPLIT_ONLY_PREDICATES,
                        "Split only on query provided predicates",
                        false,
                        false),
                booleanProperty(
                        ENABLE_STATS,
                        "Enable tiledb and connector stats per query",
                        false,
                        false),
                stringProperty(
                        TILEDB_CONFIG,
                        "TileDB config parameters in key1=value1,key2=value2 form",
                        tileDBConfig.getTileDBConfig(),
                        false),
                stringProperty(
                        ENCRYPTION_KEY,
                        "TileDB array encryption key",
                        null,
                        false),
                longProperty(
                        TIMESTAMP,
                        "TileDB array timestamp, for the time travelling feature",
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getReadBufferSize(ConnectorSession session)
    {
        return session.getProperty(READ_BUFFER_SIZE, Integer.class);
    }

    public static int getWriteBufferSize(ConnectorSession session)
    {
        return session.getProperty(WRITE_BUFFER_SIZE, Integer.class);
    }

    public static String getAwsAccessKeyId(ConnectorSession session)
    {
        return session.getProperty(AWS_ACCESS_KEY_ID, String.class);
    }

    public static String getAwsSecretAccessKey(ConnectorSession session)
    {
        return session.getProperty(AWS_SECRET_ACCESS_KEY, String.class);
    }

    public static int getSplits(ConnectorSession session)
    {
        return session.getProperty(SPLITS, Integer.class);
    }

    public static boolean getSplitOnlyPredicates(ConnectorSession session)
    {
        return session.getProperty(SPLIT_ONLY_PREDICATES, Boolean.class);
    }

    public static boolean getEnableStats(ConnectorSession session)
    {
        return session.getProperty(ENABLE_STATS, Boolean.class);
    }

    public static String getTileDBConfig(ConnectorSession session)
    {
        return session.getProperty(TILEDB_CONFIG, String.class);
    }

    public static String getEncryptionKey(ConnectorSession session)
    {
        return session.getProperty(ENCRYPTION_KEY, String.class);
    }

    public static BigInteger getTimestamp(ConnectorSession session)
    {
        if (session.getProperty(TIMESTAMP, Long.class) != null) {
            return BigInteger.valueOf(Long.valueOf(session.getProperty(TIMESTAMP, Long.class)));
        }
        else {
            return null;
        }
    }
}
