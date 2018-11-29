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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import org.apache.commons.beanutils.ConvertUtils;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_RECORD_SET_ERROR;
import static com.facebook.presto.plugin.tiledb.TileDBSessionProperties.getSplitOnlyPredicates;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.tiledb.java.api.QueryType.TILEDB_READ;
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
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
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
            try (Array array = new Array(tileDBClient.getCtx(), tableHandle.getURI().toString(), TILEDB_READ)) {
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
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = tileDBClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : tileDBClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
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
            columnHandles.put(column.getName(), new TileDBColumnHandle(connectorId, column.getName(), column.getType(), column.getIsDimension()));
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
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
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
        tileDBClient.beginCreateTable(session, tableMetadata);
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
        TileDBOutputTableHandle handle = tileDBClient.beginCreateTable(session, tableMetadata);
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
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }

        return new TileDBOutputTableHandle(
                connectorId,
                "tiledb",
                schema,
                table,
                columnNames.build(),
                columnTypes.build(),
                uri);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }
}
