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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.ArrayType;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Config;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Pair;
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

import static com.facebook.presto.plugin.tiledb.TileDBColumnProperties.getDimension;
import static com.facebook.presto.plugin.tiledb.TileDBColumnProperties.getExtent;
import static com.facebook.presto.plugin.tiledb.TileDBColumnProperties.getLowerBound;
import static com.facebook.presto.plugin.tiledb.TileDBColumnProperties.getUpperBound;
import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_CONTEXT_ERROR;
import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_CREATE_TABLE_ERROR;
import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_DROP_TABLE_ERROR;
import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_UNEXPECTED_ERROR;
import static com.facebook.presto.plugin.tiledb.TileDBModule.tileDBTypeFromPrestoType;
import static com.facebook.presto.plugin.tiledb.TileDBSessionProperties.getAwsAccessKeyId;
import static com.facebook.presto.plugin.tiledb.TileDBSessionProperties.getAwsSecretAccessKey;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Constants.TILEDB_VAR_NUM;
import static io.tiledb.java.api.Types.getJavaType;
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
            throw new PrestoException(TILEDB_CONTEXT_ERROR, tileDBError);
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
                throw new PrestoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
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
     * @return table object
     */
    public TileDBTable getTable(ConnectorSession session, String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, TileDBTable> tables = schemas.get(schema);
        // If the table does not already exists we should try to see if the user is passing an array uri
        if (tables == null || tables.get(tableName) == null) {
            try {
                Context localCtx = buildContext(session);
                return addTableFromURI(localCtx, schema, new URI(tableName));
            }
            catch (URISyntaxException e) {
                throw new PrestoException(TILEDB_UNEXPECTED_ERROR, e);
            }
            catch (TileDBError tileDBError) {
                throw new PrestoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
            }
        }
        return tables.get(tableName);
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
     * Create a array given a presto table layout/schema
     * @param tableMetadata metadata about table
     * @return Output table handler
     */
    public TileDBOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // Get schema/table names
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        try {
            Context localCtx = buildContext(session);

            // Get URI from table properties
            String uri = (String) tableMetadata.getProperties().get(TileDBTableProperties.URI);
            ArrayType arrayType;
            // Get array type from table properties
            String arrayTypeStr = ((String) tableMetadata.getProperties().get(TileDBTableProperties.ArrayType)).toUpperCase();

            // Set array type based on string value
            if (arrayTypeStr.equals("DENSE")) {
                arrayType = TILEDB_DENSE;
            }
            else if (arrayTypeStr.equals("SPARSE")) {
                arrayType = TILEDB_SPARSE;
            }
            else {
                throw new TileDBError("Invalid array type set, must be one of [DENSE, SPARSE]");
            }

            // Create array schema
            ArraySchema arraySchema = new ArraySchema(localCtx, arrayType);
            Domain domain = new Domain(localCtx);

            // If we have a sparse array we need to set capacity
            if (arrayType == TILEDB_SPARSE) {
                arraySchema.setCapacity((long) tableMetadata.getProperties().get(TileDBTableProperties.Capacity));
            }

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            // Loop through each column
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                Map<String, Object> columnProperties = column.getProperties();

                // Get column type, convert to type types
                Datatype type = tileDBTypeFromPrestoType(column.getType());
                Class classType = getJavaType(type);
                // Check if dimension or attribute
                if (getDimension(columnProperties)) {
                    Long lowerBound = getLowerBound(columnProperties);
                    Long upperBound = getUpperBound(columnProperties);
                    Long extent = getExtent(columnProperties);
                    // Switch on dimension type to convert the Long value to appropriate type
                    // If the value given by the user is too larger we set it to the max - 1
                    // for the datatype. Eventually we will error to the user with verbose details
                    // instead of altering the values
                    switch (type) {
                        case TILEDB_INT8:
                            if (upperBound > Byte.MAX_VALUE) {
                                upperBound = (long) Byte.MAX_VALUE - 1;
                            }
                            else if (upperBound < Byte.MIN_VALUE) {
                                upperBound = (long) Byte.MIN_VALUE + 1;
                            }
                            if (lowerBound > Byte.MAX_VALUE) {
                                lowerBound = (long) Byte.MAX_VALUE - 1;
                            }
                            else if (lowerBound < Byte.MIN_VALUE) {
                                lowerBound = (long) Byte.MIN_VALUE;
                            }
                            if (extent > Byte.MAX_VALUE) {
                                extent = (long) Byte.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(ctx, columnName, classType, new Pair(lowerBound.byteValue(), upperBound.byteValue()), extent.byteValue()));
                            break;
                        case TILEDB_INT16:
                            if (upperBound > Short.MAX_VALUE) {
                                upperBound = (long) Short.MAX_VALUE - 1;
                            }
                            else if (upperBound < Short.MIN_VALUE) {
                                upperBound = (long) Short.MIN_VALUE + 1;
                            }
                            if (lowerBound > Short.MAX_VALUE) {
                                lowerBound = (long) Short.MAX_VALUE - 1;
                            }
                            else if (lowerBound < Short.MIN_VALUE) {
                                lowerBound = (long) Short.MIN_VALUE;
                            }
                            if (extent > Short.MAX_VALUE) {
                                extent = (long) Short.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(ctx, columnName, classType, new Pair(lowerBound.shortValue(), upperBound.shortValue()), extent.shortValue()));
                            break;
                        case TILEDB_INT32:
                            if (upperBound > Integer.MAX_VALUE) {
                                upperBound = (long) Integer.MAX_VALUE - 1;
                            }
                            else if (upperBound < Integer.MIN_VALUE) {
                                upperBound = (long) Integer.MIN_VALUE + 1;
                            }
                            if (lowerBound > Integer.MAX_VALUE) {
                                lowerBound = (long) Integer.MAX_VALUE - 1;
                            }
                            else if (lowerBound < Integer.MIN_VALUE) {
                                lowerBound = (long) Integer.MIN_VALUE;
                            }

                            if (extent > Integer.MAX_VALUE) {
                                extent = (long) Integer.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(ctx, columnName, classType, new Pair(lowerBound.intValue(), upperBound.intValue()), extent.intValue()));
                            break;
                        case TILEDB_INT64:
                            domain.addDimension(new Dimension(ctx, columnName, classType, new Pair(lowerBound, upperBound), extent));
                            break;
                        case TILEDB_FLOAT32:
                            if (upperBound > Float.MAX_VALUE) {
                                upperBound = (long) Float.MAX_VALUE - 1;
                            }
                            else if (upperBound < Float.MIN_VALUE) {
                                upperBound = (long) Float.MIN_VALUE + 1;
                            }
                            if (lowerBound > Float.MAX_VALUE) {
                                lowerBound = (long) Float.MAX_VALUE - 1;
                            }
                            else if (lowerBound < Float.MIN_VALUE) {
                                lowerBound = (long) Float.MIN_VALUE;
                            }
                            if (extent > Float.MAX_VALUE) {
                                extent = (long) Float.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(ctx, columnName, classType, new Pair(lowerBound.floatValue(), upperBound.floatValue()), extent.floatValue()));
                            break;
                        case TILEDB_FLOAT64:
                            if (upperBound > Double.MAX_VALUE) {
                                upperBound = (long) Double.MAX_VALUE - 1;
                            }
                            else if (upperBound < Double.MIN_VALUE) {
                                upperBound = (long) Double.MIN_VALUE + 1;
                            }
                            if (lowerBound > Double.MAX_VALUE) {
                                lowerBound = (long) Double.MAX_VALUE - 1;
                            }
                            else if (lowerBound < Double.MIN_VALUE) {
                                lowerBound = (long) Double.MIN_VALUE;
                            }
                            if (extent > Double.MAX_VALUE) {
                                extent = (long) Double.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(ctx, columnName, classType, new Pair(lowerBound.doubleValue(), upperBound.doubleValue()), extent.doubleValue()));
                            break;
                        default:
                            throw new TileDBError("Invalid dimension datatype order, must be one of [TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE]");
                    }
                }
                else {
                    Attribute attribute = new Attribute(localCtx, columnName, classType);
                    if (isVarcharType(column.getType())) {
                        VarcharType varcharType = (VarcharType) column.getType();
                        if (varcharType.isUnbounded() || varcharType.getLengthSafe() > 1) {
                            attribute.setCellValNum(TILEDB_VAR_NUM);
                        }
                    }
                    else if (column.getType().equals(DATE)) {
                        attribute.setCellValNum(TILEDB_VAR_NUM);
                    }
                    arraySchema.addAttribute(attribute);
                }

                columnNames.add(columnName);
                columnTypes.add(column.getType());
            }

            // Set cell and tile order
            String cellOrderStr = ((String) tableMetadata.getProperties().get(TileDBTableProperties.CellOrder)).toUpperCase();
            String tileOrderStr = ((String) tableMetadata.getProperties().get(TileDBTableProperties.TileOrder)).toUpperCase();

            switch (cellOrderStr) {
                case "ROW_MAJOR":
                    arraySchema.setCellOrder(Layout.TILEDB_ROW_MAJOR);
                    break;
                case "COL_MAJOR":
                    arraySchema.setCellOrder(Layout.TILEDB_COL_MAJOR);
                default:
                    throw new TileDBError("Invalid cell order, must be one of [ROW_MAJOR, COL_MAJOR]");
            }

            switch (tileOrderStr) {
                case "ROW_MAJOR":
                    arraySchema.setTileOrder(Layout.TILEDB_ROW_MAJOR);
                    break;
                case "COL_MAJOR":
                    arraySchema.setTileOrder(Layout.TILEDB_COL_MAJOR);
                default:
                    throw new TileDBError("Invalid tile order, must be one of [ROW_MAJOR, COL_MAJOR]");
            }

            // Add domain
            arraySchema.setDomain(domain);

            // Validate schema
            arraySchema.check();

            Array.create(uri, arraySchema);

            // Clean up
            domain.close();
            arraySchema.close();

            addTableFromURI(localCtx, schema, new URI(uri));

            return new TileDBOutputTableHandle(
                    connectorId,
                    "tiledb",
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    uri);
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_CREATE_TABLE_ERROR, tileDBError);
        }
        catch (URISyntaxException e) {
            throw new PrestoException(TILEDB_CREATE_TABLE_ERROR, e);
        }
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
            throw new PrestoException(TILEDB_DROP_TABLE_ERROR, tileDBError);
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
            throw new PrestoException(TILEDB_DROP_TABLE_ERROR, tileDBError);
        }
    }

    public Context buildContext(ConnectorSession session) throws TileDBError
    {
        Context localCtx = ctx;
        if (session != null) {
            String awsAccessKeyId = getAwsAccessKeyId(session);
            String awsSecretAccessKey = getAwsSecretAccessKey(session);
            if (awsAccessKeyId != null && !awsAccessKeyId.isEmpty()
                    && awsSecretAccessKey != null && !awsSecretAccessKey.isEmpty()) {
                Config tileDBConfig = new Config();
                tileDBConfig.set("vfs.s3.aws_access_key_id", awsAccessKeyId);
                tileDBConfig.set("vfs.s3.aws_secret_access_key", awsSecretAccessKey);
                localCtx = new Context(tileDBConfig);
                tileDBConfig.close();
            }
        }
        return localCtx;
    }
}
