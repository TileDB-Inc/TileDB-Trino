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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * TileDBRecordSetProvider allows a record set to be fetched. Since tiledb does not have transactions, it simply
 * passes the split, column handles and a client instance to TileDBRecordSet
 */
public class TileDBRecordSetProvider
        implements ConnectorRecordSetProvider
{
    //private final String connectorId;
    private final TileDBClient tileDBClient;

    @Inject
    public TileDBRecordSetProvider(TileDBClient tileDBClient)
    {
        this.tileDBClient = requireNonNull(tileDBClient, "tileDBClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        TileDBSplit tileDBSplit = (TileDBSplit) split;

        ImmutableList.Builder<TileDBColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((TileDBColumnHandle) handle);
        }

        return new TileDBRecordSet(tileDBClient, session, tileDBSplit, handles.build());
    }
}
