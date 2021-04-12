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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_UNEXPECTED_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * TileDBConnector represents a "live" connection, it starts a transaction (which does not exist in tiledb)
 * and it stores the record set provider which is used for fetching actual results
 */
public class TileDBConnector
        implements Connector
{
    private static final Logger log = Logger.get(TileDBConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final TileDBMetadata metadata;
    private final TileDBSplitManager splitManager;
    private final TileDBRecordSetProvider recordSetProvider;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> columnProperties;
    private final TileDBPageSinkProvider tileDBPageSinkProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, TileDBMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public TileDBConnector(
            LifeCycleManager lifeCycleManager,
            TileDBMetadata metadata,
            TileDBSplitManager splitManager,
            TileDBRecordSetProvider recordSetProvider,
            List<PropertyMetadata<?>> sessionProperties,
            List<PropertyMetadata<?>> tableProperties,
            List<PropertyMetadata<?>> columnProperties,
            TileDBPageSinkProvider tileDBPageSinkProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null"));
        this.columnProperties = ImmutableList.copyOf(requireNonNull(columnProperties, "columnProperties is null"));
        this.tileDBPageSinkProvider = requireNonNull(tileDBPageSinkProvider, "tileDBPageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        TileDBTransactionHandle transaction = new TileDBTransactionHandle();
        transactions.put(transaction, metadata);
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transactionHandle)
    {
        checkArgument(transactions.remove(transactionHandle) != null, "no such transaction: %s", transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        TileDBMetadata metadata = transactions.remove(transactionHandle);
        checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
        metadata.rollback();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return tileDBPageSinkProvider;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
            throw new PrestoException(TILEDB_UNEXPECTED_ERROR, e);
        }
    }
}
