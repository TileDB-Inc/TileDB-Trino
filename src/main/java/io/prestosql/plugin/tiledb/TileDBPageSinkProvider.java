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

import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Provider for page sink for inserts
 */
public class TileDBPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final TileDBClient tileDBClient;

    @Inject
    public TileDBPageSinkProvider(TileDBClient tileDBClient)
    {
        this.tileDBClient = requireNonNull(tileDBClient, "tileDBClient is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        return new TileDBPageSink((TileDBOutputTableHandle) tableHandle, tileDBClient, session);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        return new TileDBPageSink((TileDBOutputTableHandle) tableHandle, tileDBClient, session);
    }
}
