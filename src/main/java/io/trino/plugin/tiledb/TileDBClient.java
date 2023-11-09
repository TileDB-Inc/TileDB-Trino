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
package io.trino.plugin.tiledb;

import com.google.common.collect.ImmutableSet;
import io.tiledb.java.api.Config;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.EncryptionType;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import jakarta.inject.Inject;
import oshi.hardware.HardwareAbstractionLayer;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.tiledb.TileDBErrorCode.TILEDB_CONFIG_ERROR;
import static io.trino.plugin.tiledb.TileDBErrorCode.TILEDB_CONTEXT_ERROR;
import static io.trino.plugin.tiledb.TileDBErrorCode.TILEDB_DROP_TABLE_ERROR;
import static io.trino.plugin.tiledb.TileDBErrorCode.TILEDB_UNEXPECTED_ERROR;
import static io.trino.plugin.tiledb.TileDBSessionProperties.getAwsAccessKeyId;
import static io.trino.plugin.tiledb.TileDBSessionProperties.getAwsSecretAccessKey;
import static io.trino.plugin.tiledb.TileDBSessionProperties.getTileDBConfig;
import static java.util.Objects.requireNonNull;

/**
 * TileDBClient purpose is to wrap around the client functionality.
 * This allows TileDB to have mechanisms to "discover" what tables/schemas exists and report it to prestodb.
 * Currently this just reads a single array uri from the config but will eventually support reading a list
 * of arrays or other discovery mechanisms.
 *
 * The client is also responsible for developing the splits. See TileDBSplit for more details
 *
 * TileDB Client holds a single context for all table, we probably want to change this.
 */
public class TileDBClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Map<String, Map<String, TileDBTable>> schemas;
    //TileDBTable tileDBTable;
    protected final String connectorId;
    protected Context ctx;

    protected TileDBConfig config;
    protected oshi.SystemInfo systemInfo;
    protected HardwareAbstractionLayer hardwareAbstractionLayer;

    private final String defaultSchema = "tiledb";

    @Inject
    public TileDBClient(TileDBConnectorId connectorId, TileDBConfig config)
    {
        // Make sure config passed is not empty, since we read array-uri from it
        this.config = requireNonNull(config, "config is null");
        try {
            // Create context
            ctx = new Context(buildConfig());
            //tileDBConfig.close();
        }
        catch (TileDBError tileDBError) {
            // Print stacktrace, this produces an error client side saying "internal error"
            throw new TrinoException(TILEDB_CONTEXT_ERROR, tileDBError);
        }

        // connectorId is an internal prestodb identified
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        // Build maps for storing schema and tables
        schemas = new HashMap<>();
        if (config.getArrayURIs() != null) {
            for (URI arrayUri : config.getArrayURIs()) {
                addTableFromURI(ctx, defaultSchema, arrayUri);
            }
        }
        if (!schemas.containsKey(defaultSchema)) {
            schemas.put(defaultSchema, new HashMap<>());
        }

        systemInfo = new oshi.SystemInfo();

        hardwareAbstractionLayer = systemInfo.getHardware();
    }

    public HardwareAbstractionLayer getHardwareAbstractionLayer()
    {
        return hardwareAbstractionLayer;
    }

    /**
     * Get plugin configuration
     *
     * @return TileDB plugin configuration
     */
    public TileDBConfig getConfig()
    {
        return this.config;
    }

    /**
     * Helper function to add a table to the catalog
     *
     * @param schema schema to add table to
     * @param arrayUri uri of array/table
     */
    public TileDBTable addTableFromURI(Context localCtx, String schema, URI arrayUri)
    {
        // Currently create the "table name" by splitting the path and grabbing the last part.
        String path = arrayUri.getPath();
        String tableName = path.substring(path.lastIndexOf('/') + 1);
        // Create a table instances
        TileDBTable tileDBTable = null;
        try {
            tileDBTable = new TileDBTable(schema, tableName, arrayUri, localCtx);
            Map<String, TileDBTable> tableMapping = schemas.get(schema);
            if (tableMapping == null) {
                tableMapping = new HashMap<>();
            }
            tableMapping.put(tableName, tileDBTable);
            schemas.put(schema, tableMapping);
        }
        catch (TileDBError tileDBError) {
            if ((new File(arrayUri.toString())).exists()) {
                throw new TrinoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
            }
        }
        return tileDBTable;
    }

    /**
     * Return list of schema's. Since we don't have true discovery this is a static map which we return the keys
     *
     * @return List of tiledb schema names (currently just "TileDB")
     */
    public Set<String> getSchemaNames()
    {
        return schemas.keySet();
    }

    /**
     * Return all "tables" in a schema
     *
     * @param schema the schema
     * @return Set of table names
     */
    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, TileDBTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    /**
     * Fetches a table object given a schema and a table name
     *
     * @param schema the schema
     * @param tableName the table name
     * @param encryptionType the encryption type
     * @param encryptionKey the encryption key
     * @return table object
     */
    public TileDBTable getTable(ConnectorSession session, String schema, String tableName,
                                EncryptionType encryptionType, String encryptionKey)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, TileDBTable> tables = schemas.get(schema);
        // If the table does not already exists we should try to see if the user is passing an array uri
        if (tables == null || tables.get(tableName) == null) {
            try {
                Context localCtx = buildContext(session, encryptionType, encryptionKey);
                return addTableFromURI(localCtx, schema, new URI(tableName));
            }
            catch (URISyntaxException e) {
                throw new TrinoException(TILEDB_UNEXPECTED_ERROR, e);
            }
            catch (TileDBError tileDBError) {
                throw new TrinoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
            }
        }
        return tables.get(tableName);
    }

    /**
     * Fetches a table object given a schema and a table name
     *
     * @param schema the schema
     * @param tableName the table name
     * @return table object
     */
    public TileDBTable getTable(ConnectorSession session, String schema, String tableName)
    {
        String key = TileDBSessionProperties.getEncryptionKey(session);
        if (key != null) {
            return this.getTable(session, schema, tableName, EncryptionType.TILEDB_AES_256_GCM, key);
        }
        else {
            return this.getTable(session, schema, tableName, null, null);
        }
    }

    public Context getCtx()
    {
        return ctx;
    }

    /**
     * Rollback a create table statement, this just drops the array
     *
     * @param handle TileDB table handler
     */
    public void rollbackCreateTable(TileDBOutputTableHandle handle)
    {
        try {
            TileDBObject.remove(ctx, handle.getURI());
            schemas.get(handle.getSchemaName()).remove(handle.getTableName());
            ctx.close();
            ctx = new Context(buildConfig());
        }
        catch (TileDBError tileDBError) {
            throw new TrinoException(TILEDB_DROP_TABLE_ERROR, tileDBError);
        }
    }

    public void dropTable(ConnectorSession session, TileDBTableHandle handle)
    {
        try {
            Context localCtx = buildContext(session, null, null);
            TileDBObject.remove(localCtx, handle.getURI());
            schemas.get(handle.getSchemaName()).remove(handle.getTableName());
            ctx.close();
            ctx = new Context(buildConfig());
        }
        catch (TileDBError tileDBError) {
            throw new TrinoException(TILEDB_DROP_TABLE_ERROR, tileDBError);
        }
    }

    public Config buildConfig()
    {
        try {
            // Create context
            Config tileDBConfig = new Config();
            if (config.getAwsAccessKeyId() != null && !config.getAwsAccessKeyId().isEmpty()
                    && config.getAwsSecretAccessKey() != null && !config.getAwsSecretAccessKey().isEmpty()) {
                tileDBConfig.set("vfs.s3.aws_access_key_id", config.getAwsAccessKeyId());
                tileDBConfig.set("vfs.s3.aws_secret_access_key", config.getAwsSecretAccessKey());
            }
            return tileDBConfig;
            //tileDBConfig.close();
        }
        catch (TileDBError tileDBError) {
            // Print stacktrace, this produces an error client side saying "internal error"
            throw new TrinoException(TILEDB_CONTEXT_ERROR, tileDBError);
        }
    }

    public Context buildContext(ConnectorSession session, EncryptionType encryptionType, String encryptionKey)
            throws TileDBError
    {
        Config tileDBConfig = new Config();
        boolean updateCtx = false;

        // Encryption
        String currentKey = ctx.getConfig().get("sm.encryption_key");
        boolean currentDifferent = !currentKey.equals(encryptionKey);
        // add a key only if the current is different
        if (encryptionType != null && encryptionKey != null && currentDifferent) {
            tileDBConfig.set("sm.encryption_type", encryptionType.toString().replace("TILEDB_", ""));
            tileDBConfig.set("sm.encryption_key", encryptionKey);
            updateCtx = true;
        }

        if (session != null) {
            String configString = getTileDBConfig(session);

            // If the user set any tiledb config parameters, we will set them here
            if (configString != null) {
                for (String config : configString.split(",")) {
                    String[] kv = config.split("=");
                    if (kv.length != 2) {
                        throw new TrinoException(TILEDB_CONFIG_ERROR, "invalid config for " + config);
                    }
                    tileDBConfig.set(kv[0], kv[1]);
                    updateCtx = true;
                }
            }

            // We'll deprecate the AWS Access Key parameters and remove in a future version
            String awsAccessKeyId = getAwsAccessKeyId(session);
            String awsSecretAccessKey = getAwsSecretAccessKey(session);
            if (awsAccessKeyId != null && !awsAccessKeyId.isEmpty()
                    && awsSecretAccessKey != null && !awsSecretAccessKey.isEmpty()) {
                tileDBConfig.set("vfs.s3.aws_access_key_id", awsAccessKeyId);
                tileDBConfig.set("vfs.s3.aws_secret_access_key", awsSecretAccessKey);
                updateCtx = true;
            }
            if (updateCtx) {
                ctx = new Context(tileDBConfig);
            }
            tileDBConfig.close();
        }
        return ctx;
    }
}
