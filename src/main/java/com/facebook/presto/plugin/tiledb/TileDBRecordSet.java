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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.EncryptionType;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.TileDBError;

import java.math.BigInteger;
import java.util.List;

import static com.facebook.presto.plugin.tiledb.TileDBSessionProperties.getEncryptionKey;
import static com.facebook.presto.plugin.tiledb.TileDBSessionProperties.getTimestamp;
import static io.tiledb.java.api.QueryType.TILEDB_READ;
import static java.util.Objects.requireNonNull;

/**
 * TileDBRecordSet is responsible for create the records set to retrieve. Currently it create a query
 * and array object then allows the record cursor to do the actual querying/buffer handling
 */
public class TileDBRecordSet
        implements RecordSet
{
    private final TileDBClient tileDBClient;
    private final ConnectorSession session;
    private final List<TileDBColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final TileDBSplit split;
    private Query query;
    private Array array;

    public TileDBRecordSet(TileDBClient tileDBClient, ConnectorSession session, TileDBSplit split, List<TileDBColumnHandle> columnHandles)
    {
        this.tileDBClient = requireNonNull(tileDBClient, "tileDBClient is null");
        this.session = requireNonNull(session, "session is null");
        this.split = requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TileDBColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        TileDBTable table = tileDBClient.getTable(session, split.getSchemaName(), split.getTableName());
        requireNonNull(table, "Unable to fetch table " + split.getSchemaName() + "." + split.getTableName() + " for record set");
        try {
            String key = getEncryptionKey(session);
            BigInteger timestamp = getTimestamp(session);

            if (key != null && timestamp != null) {
                array = new Array(tileDBClient.buildContext(session), table.getURI().toString(), TILEDB_READ, EncryptionType.TILEDB_AES_256_GCM, key.getBytes(), timestamp);
            }
            else if (key != null) {
                array = new Array(tileDBClient.buildContext(session), table.getURI().toString(), TILEDB_READ, EncryptionType.TILEDB_AES_256_GCM, key.getBytes());
            }
            else if (timestamp != null) {
                array = new Array(tileDBClient.buildContext(session), table.getURI().toString(), TILEDB_READ, timestamp);
            }
            else {
                array = new Array(tileDBClient.buildContext(session), table.getURI().toString(), TILEDB_READ);
            }
            query = new Query(array, TILEDB_READ);
            query.setLayout(Layout.TILEDB_ROW_MAJOR);
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TileDBErrorCode.TILEDB_RECORD_SET_ERROR, tileDBError);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TileDBRecordCursor(tileDBClient, session, split, columnHandles, array, query);
    }
}
