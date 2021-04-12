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
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import io.tiledb.java.api.Config;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.EncryptionType;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import oshi.hardware.HardwareAbstractionLayer;

import javax.inject.Inject;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
            Config tileDBConfig = new Config();
            if (config.getAwsAccessKeyId() != null && !config.getAwsAccessKeyId().isEmpty()
                    && config.getAwsSecretAccessKey() != null && !config.getAwsSecretAccessKey().isEmpty()) {
                tileDBConfig.set("vfs.s3.aws_access_key_id", config.getAwsAccessKeyId());
                tileDBConfig.set("vfs.s3.aws_secret_access_key", config.getAwsSecretAccessKey());
            }
            ctx = new Context(tileDBConfig);
            //tileDBConfig.close();
        }
        catch (TileDBError tileDBError) {
            // Print stacktrace, this produces an error client side saying "internal error"
            throw new PrestoException(TileDBErrorCode.TILEDB_CONTEXT_ERROR, tileDBError);
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

    /**
     * Get plugin configuration
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
        return addTableFromURI(localCtx, schema, arrayUri, null, null);
    }

    /**
     * Helper function to add a table to the catalog
     * @param schema schema to add table to
     * @param arrayUri uri of array/table
     * @param encryptionType The encryption type
     * @param encryptionKey The encryption key in bytes
     */
    public TileDBTable addTableFromURI(Context localCtx, String schema, URI arrayUri, EncryptionType encryptionType,
                                       byte[] encryptionKey)
    {
        // Currently create the "table name" by splitting the path and grabbing the last part.
        String path = arrayUri.getPath();
        String tableName = path.substring(path.lastIndexOf('/') + 1);
        // Create a table instances
        TileDBTable tileDBTable = null;
        try {
            tileDBTable = new TileDBTable(schema, tableName, arrayUri, localCtx, encryptionType, encryptionKey);
            Map<String, TileDBTable> tableMapping = schemas.get(schema);
            if (tableMapping == null) {
                tableMapping = new HashMap<>();
            }
            tableMapping.put(tableName, tileDBTable);
            schemas.put(schema, tableMapping);
        }
        catch (TileDBError tileDBError) {
            if ((new File(arrayUri.toString())).exists()) {
                throw new PrestoException(TileDBErrorCode.TILEDB_UNEXPECTED_ERROR, tileDBError);
            }
        }
        return tileDBTable;
    }

    /**
     * Return list of schema's. Since we don't have true discovery this is a static map which we return the keys
     * @return List of tiledb schema names (currently just "tiledb")
     */
    public Set<String> getSchemaNames()
    {
        return schemas.keySet();
    }

    /**
     * Return all "tables" in a schema
     * @param schema
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
     * @param schema
     * @param tableName
     * @param encryptionType
     * @param encryptionKey
     * @return table object
     */
    public TileDBTable getTable(ConnectorSession session, String schema, String tableName,
                                EncryptionType encryptionType, byte[] encryptionKey)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, TileDBTable> tables = schemas.get(schema);
        // If the table does not already exists we should try to see if the user is passing an array uri
        if (tables == null || tables.get(tableName) == null) {
            try {
                Context localCtx = buildContext(session);
                return addTableFromURI(localCtx, schema, new URI(tableName), encryptionType, encryptionKey);
            }
            catch (URISyntaxException e) {
                throw new PrestoException(TileDBErrorCode.TILEDB_UNEXPECTED_ERROR, e);
            }
            catch (TileDBError tileDBError) {
                throw new PrestoException(TileDBErrorCode.TILEDB_UNEXPECTED_ERROR, tileDBError);
            }
        }
        return tables.get(tableName);
    }

    /**
     * Fetches a table object given a schema and a table name
     * @param schema
     * @param tableName
     * @return table object
     */
    public TileDBTable getTable(ConnectorSession session, String schema, String tableName)
    {
        String key = TileDBSessionProperties.getEncryptionKey(session);
        if (key != null) {
            return this.getTable(session, schema, tableName, EncryptionType.TILEDB_AES_256_GCM, key.getBytes());
        }
        else {
            return this.getTable(session, schema, tableName, null, null);
        }
    }

    public Context getCtx()
    {
        return ctx;
    }

    public HardwareAbstractionLayer getHardwareAbstractionLayer()
    {
        return hardwareAbstractionLayer;
    }

    /**
     * Rollback a create table statement, this just drops the array
     * @param handle tiledb table handler
     */
    public void rollbackCreateTable(TileDBOutputTableHandle handle)
    {
        try {
            TileDBObject.remove(ctx, handle.getURI());
            schemas.get(handle.getSchemaName()).remove(handle.getTableName());
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TileDBErrorCode.TILEDB_DROP_TABLE_ERROR, tileDBError);
        }
    }

    public void dropTable(ConnectorSession session, TileDBTableHandle handle)
    {
        try {
            Context localCtx = buildContext(session);
            TileDBObject.remove(localCtx, handle.getURI());
            schemas.get(handle.getSchemaName()).remove(handle.getTableName());
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TileDBErrorCode.TILEDB_DROP_TABLE_ERROR, tileDBError);
        }
    }

    public Context buildContext(ConnectorSession session) throws TileDBError
    {
        Context localCtx = ctx;
        if (session != null) {
            String configString = TileDBSessionProperties.getTileDBConfig(session);
            Config tileDBConfig = new Config();
            boolean updateCtx = false;

            // If the user set any tiledb config parameters, we will set them here
            if (configString != null) {
                for (String config : configString.split(",")) {
                    String[] kv = config.split("=");
                    if (kv.length != 2) {
                        throw new PrestoException(TileDBErrorCode.TILEDB_CONFIG_ERROR, "invalid config for " + config);
                    }
                    tileDBConfig.set(kv[0], kv[1]);
                    updateCtx = true;
                }
            }

            // We'll deprecate the AWS Access Key parameters and remove in a future version
            String awsAccessKeyId = TileDBSessionProperties.getAwsAccessKeyId(session);
            String awsSecretAccessKey = TileDBSessionProperties.getAwsSecretAccessKey(session);
            if (awsAccessKeyId != null && !awsAccessKeyId.isEmpty()
                    && awsSecretAccessKey != null && !awsSecretAccessKey.isEmpty()) {
                tileDBConfig.set("vfs.s3.aws_access_key_id", awsAccessKeyId);
                tileDBConfig.set("vfs.s3.aws_secret_access_key", awsSecretAccessKey);
                updateCtx = true;
            }
            if (updateCtx) {
                localCtx = new Context(tileDBConfig);
            }
            tileDBConfig.close();
        }
        return localCtx;
    }
}
