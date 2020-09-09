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
package io.prestosql.plugin.tiledb;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.LocalProperty;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.ArrayType;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import org.apache.commons.beanutils.ConvertUtils;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.tiledb.TileDBColumnProperties.getDimension;
import static io.prestosql.plugin.tiledb.TileDBColumnProperties.getExtent;
import static io.prestosql.plugin.tiledb.TileDBColumnProperties.getLowerBound;
import static io.prestosql.plugin.tiledb.TileDBColumnProperties.getUpperBound;
import static io.prestosql.plugin.tiledb.TileDBErrorCode.TILEDB_CREATE_TABLE_ERROR;
import static io.prestosql.plugin.tiledb.TileDBErrorCode.TILEDB_RECORD_SET_ERROR;
import static io.prestosql.plugin.tiledb.TileDBModule.tileDBTypeFromPrestoType;
import static io.prestosql.plugin.tiledb.TileDBSessionProperties.getSplitOnlyPredicates;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Constants.TILEDB_VAR_NUM;
import static io.tiledb.java.api.QueryType.TILEDB_READ;
import static io.tiledb.java.api.Types.getJavaType;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

/**
 * TileDBMetadata provides information (metadata) to prestodb for tiledb arrays. This includes fetching table
 * create structures, columns lists, etc. It return most of this data in native prestodb classes,
 * such as `ColumnMetadata` class
 */
public class TileDBMetadata
        implements ConnectorMetadata
{
    private final String connectorId;

    private final TileDBClient tileDBClient;

    // Rollback stores a function to run to initiate a rollback sequence
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    @Inject
    public TileDBMetadata(TileDBConnectorId connectorId, TileDBClient tileDBClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.tileDBClient = requireNonNull(tileDBClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(tileDBClient.getSchemaNames());
    }

    @Override
    public TileDBTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        TileDBTable table = tileDBClient.getTable(session, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new TileDBTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName(), table.getURI().toString());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        TileDBTableHandle tableHandle = (TileDBTableHandle) table;

        // Set the dimensions as the partition columns
        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        ImmutableList.Builder<LocalProperty<ColumnHandle>> localProperties = ImmutableList.builder();

        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);

        // Predicates are fetched as summary of constraints
        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary();
        Set<ColumnHandle> dimensionHandles = columns.values().stream()
                .filter(e -> ((TileDBColumnHandle) e).getIsDimension())
                .collect(Collectors.toSet());

        List<ColumnHandle> columnsInLayout;
        if (desiredColumns.isPresent()) {
            // Add all dimensions since dimensions will always be returned by tiledb
            Set<ColumnHandle> desiredColumnsWithDimension = new HashSet<>(desiredColumns.get());
            desiredColumnsWithDimension.addAll(dimensionHandles);
            columnsInLayout = new ArrayList<>(desiredColumnsWithDimension);
        }
        else {
            columnsInLayout = new ArrayList<>(columns.values());
        }

        // The only enforceable constraints are ones for dimension columns
        Map<ColumnHandle, Domain> enforceableDimensionDomains = new HashMap<>(Maps.filterKeys(effectivePredicate.getDomains().get(), Predicates.in(dimensionHandles)));

        if (!getSplitOnlyPredicates(session)) {
            try (Array array = new Array(tileDBClient.buildContext(session), tableHandle.getURI().toString(), TILEDB_READ)) {
                HashMap<String, Pair> nonEmptyDomain = array.nonEmptyDomain();
                // Find any dimension which do not have predicates and add one for the entire domain.
                // This is required so we can later split on the predicates
                for (ColumnHandle dimensionHandle : dimensionHandles) {
                    if (!enforceableDimensionDomains.containsKey(dimensionHandle)) {
                        TileDBColumnHandle columnHandle = ((TileDBColumnHandle) dimensionHandle);
                        if (nonEmptyDomain.containsKey(columnHandle.getColumnName())) {
                            Pair<Object, Object> domain = nonEmptyDomain.get(columnHandle.getColumnName());
                            Object nonEmptyMin = domain.getFirst();
                            Object nonEmptyMax = domain.getSecond();
                            Type type = columnHandle.getColumnType();
                            if (nonEmptyMin == null || nonEmptyMax == null) {
                                continue;
                            }

                            Range range;
                            if (REAL.equals(type)) {
                                range = Range.range(type, ((Integer) floatToRawIntBits((Float) nonEmptyMin)).longValue(), true,
                                        ((Integer) floatToRawIntBits((Float) nonEmptyMax)).longValue(), true);
                            }
                            else {
                                range = Range.range(type,
                                        ConvertUtils.convert(nonEmptyMin, type.getJavaType()), true,
                                        ConvertUtils.convert(nonEmptyMax, type.getJavaType()), true);
                            }

                            enforceableDimensionDomains.put(
                                    dimensionHandle,
                                    Domain.create(ValueSet.ofRanges(range), false));
                        }
                    }
                }
            }
            catch (TileDBError tileDBError) {
                throw new PrestoException(TILEDB_RECORD_SET_ERROR, tileDBError);
            }
        }

        TupleDomain<ColumnHandle> enforceableTupleDomain = TupleDomain.withColumnDomains(enforceableDimensionDomains);
        TupleDomain<ColumnHandle> remainingTupleDomain;

        // The remaining tuples non-enforced by TileDB are attributes
        remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), Predicates.not(Predicates.in(dimensionHandles))));

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new TileDBTableLayoutHandle(tableHandle, enforceableTupleDomain, dimensionHandles),
                Optional.of(columnsInLayout),
                TupleDomain.all(),
                Optional.empty(),
                partitioningColumns,
                Optional.empty(),
                localProperties.build());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, remainingTupleDomain));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TileDBTableLayoutHandle layout = (TileDBTableLayoutHandle) handle;

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        TileDBTableHandle tileDBTableHandle = (TileDBTableHandle) table;
        checkArgument(tileDBTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(tileDBTableHandle.getSchemaName(), tileDBTableHandle.getTableName());

        return getTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        Set<String> schemaNames;
        if (schemaName.isPresent()) {
            schemaNames = ImmutableSet.of(schemaName.get());
        }
        else {
            schemaNames = tileDBClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schema : schemaNames) {
            for (String tableName : tileDBClient.getTableNames(schema)) {
                builder.add(new SchemaTableName(schema, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TileDBTableHandle tileDBTableHandle = (TileDBTableHandle) tableHandle;
        checkArgument(tileDBTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        TileDBTable table = tileDBClient.getTable(session, tileDBTableHandle.getSchemaName(), tileDBTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(tileDBTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (TileDBColumn column : table.getColumns()) {
            // Create column handles, extra info contains a boolean for if its a dimension (true) or attribute (false)
            columnHandles.put(column.getName(), new TileDBColumnHandle(connectorId, column.getName(), column.getType(), column.getTileDBType(), column.getIsVariableLength(), column.getIsDimension()));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        TileDBTable table = tileDBClient.getTable(session, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(new SchemaTableName(tableName.getSchemaName(), tableName.getTableName()));
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isPresent()) {
            return listTables(session, prefix.getSchema());
        }
        if (prefix.getSchema().isPresent() && prefix.getTable().isPresent()) {
            return ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
        }
        else {
            return Collections.emptyList();
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((TileDBColumnHandle) columnHandle).getColumnMetadata();
    }

    /**
     *  Create table creates a table without any data
     * @param session connector session
     * @param tableMetadata metadata for new table
     * @param ignoreExisting ignore existing tables? Currently not supported
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        beginCreateArray(session, tableMetadata);
    }

    /**
     * beginCreateTable creates a table with data
     * @param session connector sessions
     * @param tableMetadata metadata for table
     * @param layout layout of new table
     * @return output table handles
     */
    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        TileDBOutputTableHandle handle = beginCreateArray(session, tableMetadata);
        setRollback(() -> tileDBClient.rollbackCreateTable(handle));
        return handle;
    }

    /**
     * Finish/commit creating a table with data
     * @param session connector session
     * @param tableHandle table handle
     * @param fragments any fragements (ignored)
     * @param computedStatistics (ignored)
     * @return Resulting metadata if any
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        // Since tileDB does not have transactions, and becuase the TileDBOutputTableHandle must be json serializable
        // we effectively commit the create table in beginCreateTable. Only this this does is clear the rollback
        clearRollback();
        return Optional.empty();
    }

    /**
     * Set a rollback for a method to run some function at the rollback of a presto trasnaction
     * @param action
     */
    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    /**
     * Remove any configured rollbacks
     */
    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    /**
     * Run a rollback
     */
    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    /**
     * Allow dropping of a table/tiledb array
     * @param session connector session
     * @param tableHandle handle of table to be dropped
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        tileDBClient.dropTable(session, (TileDBTableHandle) tableHandle);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // Get schema/table names
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        TileDBTableHandle tileDBTableHandle = (TileDBTableHandle) tableHandle;
        // Try to get uri from handle if that fails try properties
        String uri = tileDBTableHandle.getURI();
        if (uri.isEmpty()) {
            uri = (String) tableMetadata.getProperties().get(TileDBTableProperties.URI);
        }
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        // Loop through all columns and build list of column handles in the proper ordering. Order is important here
        // because we will use the list to avoid hashmap lookups for better performance.
        List<ColumnMetadata> columnMetadata = tableMetadata.getColumns();
        List<TileDBColumnHandle> columnHandles = new ArrayList<>(Collections.nCopies(columnMetadata.size(), null));
        for (Map.Entry<String, ColumnHandle> columnHandleSet : getColumnHandles(session, tableHandle).entrySet()) {
            for (int i = 0; i < columnMetadata.size(); i++) {
                if (columnHandleSet.getKey().toLowerCase(Locale.ENGLISH).equals(columnMetadata.get(i).getName())) {
                    columnHandles.set(i, (TileDBColumnHandle) columnHandleSet.getValue());
                }
            }
        }

        return new TileDBOutputTableHandle(
                connectorId,
                "tiledb",
                schema,
                table,
                columnHandles,
                uri);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    /**
     * Create an array given a presto table layout/schema
     * @param tableMetadata metadata about table
     * @return Output table handler
     */
    public TileDBOutputTableHandle beginCreateArray(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        // Get schema/table names
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        try {
            Context localCtx = tileDBClient.buildContext(session);

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
            io.tiledb.java.api.Domain domain = new io.tiledb.java.api.Domain(localCtx);

            // If we have a sparse array we need to set capacity
            if (arrayType == TILEDB_SPARSE) {
                arraySchema.setCapacity((long) tableMetadata.getProperties().get(TileDBTableProperties.Capacity));
            }

            List<String> columnNames = new ArrayList<>();
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
                            if (extent > Byte.MAX_VALUE) {
                                extent = 10000L;
                            }
                            if (upperBound > Byte.MAX_VALUE - extent) {
                                upperBound = (long) Byte.MAX_VALUE - extent;
                            }
                            else if (upperBound < Byte.MIN_VALUE) {
                                upperBound = (long) Byte.MIN_VALUE + extent;
                            }
                            if (lowerBound > Byte.MAX_VALUE) {
                                lowerBound = (long) Byte.MAX_VALUE - extent;
                            }
                            else if (lowerBound < Byte.MIN_VALUE) {
                                lowerBound = (long) Byte.MIN_VALUE;
                            }
                            domain.addDimension(new Dimension(localCtx, columnName, classType, new Pair(lowerBound.byteValue(), upperBound.byteValue()), extent.byteValue()));
                            break;
                        case TILEDB_INT16:
                            if (extent > Short.MAX_VALUE) {
                                extent = 10000L;
                            }
                            if (upperBound > Short.MAX_VALUE - extent) {
                                upperBound = (long) Short.MAX_VALUE - extent;
                            }
                            else if (upperBound < Short.MIN_VALUE) {
                                upperBound = (long) Short.MIN_VALUE + extent;
                            }
                            if (lowerBound > Short.MAX_VALUE) {
                                lowerBound = (long) Short.MAX_VALUE - extent;
                            }
                            else if (lowerBound < Short.MIN_VALUE) {
                                lowerBound = (long) Short.MIN_VALUE;
                            }
                            domain.addDimension(new Dimension(localCtx, columnName, classType, new Pair(lowerBound.shortValue(), upperBound.shortValue()), extent.shortValue()));
                            break;
                        case TILEDB_INT32:
                            if (extent > Integer.MAX_VALUE) {
                                extent = 10000L;
                            }
                            if (upperBound > Integer.MAX_VALUE - extent) {
                                upperBound = (long) Integer.MAX_VALUE - extent;
                            }
                            else if (upperBound < Integer.MIN_VALUE) {
                                upperBound = (long) Integer.MIN_VALUE + extent;
                            }
                            if (lowerBound > Integer.MAX_VALUE) {
                                lowerBound = (long) Integer.MAX_VALUE - extent;
                            }
                            else if (lowerBound < Integer.MIN_VALUE) {
                                lowerBound = (long) Integer.MIN_VALUE;
                            }
                            domain.addDimension(new Dimension(localCtx, columnName, classType, new Pair(lowerBound.intValue(), upperBound.intValue()), extent.intValue()));
                            break;
                        case TILEDB_DATETIME_AS:
                        case TILEDB_DATETIME_FS:
                        case TILEDB_DATETIME_PS:
                        case TILEDB_DATETIME_NS:
                        case TILEDB_DATETIME_US:
                        case TILEDB_DATETIME_MS:
                        case TILEDB_DATETIME_SEC:
                        case TILEDB_DATETIME_MIN:
                        case TILEDB_DATETIME_HR:
                        case TILEDB_DATETIME_DAY:
                        case TILEDB_DATETIME_WEEK:
                        case TILEDB_DATETIME_MONTH:
                        case TILEDB_DATETIME_YEAR:
                        case TILEDB_INT64:
                            if (upperBound > Long.MAX_VALUE - extent) {
                                upperBound = (long) Long.MAX_VALUE - extent;
                            }
                            domain.addDimension(new Dimension(localCtx, columnName, type, new Pair(lowerBound, upperBound), extent));
                            break;
                        case TILEDB_FLOAT32:
                            if (upperBound > Float.MAX_VALUE - extent) {
                                upperBound = (long) Float.MAX_VALUE - extent;
                            }
                            else if (upperBound < Float.MIN_VALUE) {
                                upperBound = (long) Float.MIN_VALUE + extent;
                            }
                            if (lowerBound > Float.MAX_VALUE) {
                                lowerBound = (long) Float.MAX_VALUE - extent;
                            }
                            else if (lowerBound < Float.MIN_VALUE) {
                                lowerBound = (long) Float.MIN_VALUE;
                            }
                            if (extent > Float.MAX_VALUE) {
                                extent = (long) Float.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(localCtx, columnName, classType, new Pair(lowerBound.floatValue(), upperBound.floatValue()), extent.floatValue()));
                            break;
                        case TILEDB_FLOAT64:
                            if (upperBound > Double.MAX_VALUE - extent) {
                                upperBound = (long) Double.MAX_VALUE - extent;
                            }
                            else if (upperBound < Double.MIN_VALUE) {
                                upperBound = (long) Double.MIN_VALUE + extent;
                            }
                            if (lowerBound > Double.MAX_VALUE) {
                                lowerBound = (long) Double.MAX_VALUE - extent;
                            }
                            else if (lowerBound < Double.MIN_VALUE) {
                                lowerBound = (long) Double.MIN_VALUE;
                            }
                            if (extent > Double.MAX_VALUE) {
                                extent = (long) Double.MAX_VALUE;
                            }
                            domain.addDimension(new Dimension(localCtx, columnName, classType, new Pair(lowerBound.doubleValue(), upperBound.doubleValue()), extent.doubleValue()));
                            break;
                        default:
                            throw new TileDBError("Invalid dimension datatype order, must be one of [TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE]");
                    }
                }
                else {
                    Attribute attribute = new Attribute(localCtx, columnName, type);
                    if (isVarcharType(column.getType())) {
                        VarcharType varcharType = (VarcharType) column.getType();
                        Optional<Integer> len = varcharType.getLength();
                        if (varcharType.isUnbounded() || (len.isPresent() && len.get() > 1)) {
                            attribute.setCellValNum(TILEDB_VAR_NUM);
                        }
                    }
                    arraySchema.addAttribute(attribute);
                }

                columnNames.add(columnName);
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
                    break;
                default:
                    throw new TileDBError("Invalid cell order, must be one of [ROW_MAJOR, COL_MAJOR]");
            }

            switch (tileOrderStr) {
                case "ROW_MAJOR":
                    arraySchema.setTileOrder(Layout.TILEDB_ROW_MAJOR);
                    break;
                case "COL_MAJOR":
                    arraySchema.setTileOrder(Layout.TILEDB_COL_MAJOR);
                    break;
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

            TileDBTable tileDBTable = tileDBClient.addTableFromURI(localCtx, schema, new URI(uri));

            // Loop through all columns and build list of column handles in the proper ordering. Order is important here
            // because we will use the list to avoid hashmap lookups for better performance.
            List<TileDBColumnHandle> columnHandles = new ArrayList<>(Collections.nCopies(columnNames.size(), null));
            for (TileDBColumn column : tileDBTable.getColumns()) {
                for (int i = 0; i < columnNames.size(); i++) {
                    if (column.getName().toLowerCase(Locale.ENGLISH).equals(columnNames.get(i))) {
                        columnHandles.set(i, new TileDBColumnHandle(connectorId, column.getName(), column.getType(), column.getTileDBType(), column.getIsVariableLength(), column.getIsDimension()));
                    }
                }
            }

            return new TileDBOutputTableHandle(
                    connectorId,
                    "tiledb",
                    schema,
                    table,
                    columnHandles,
                    uri);
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_CREATE_TABLE_ERROR, tileDBError);
        }
        catch (URISyntaxException e) {
            throw new PrestoException(TILEDB_CREATE_TABLE_ERROR, e);
        }
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return true;
    }
}
