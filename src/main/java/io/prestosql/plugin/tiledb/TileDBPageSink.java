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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryStatus;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.prestosql.plugin.tiledb.TileDBErrorCode.TILEDB_PAGE_SINK_ERROR;
import static io.prestosql.plugin.tiledb.TileDBSessionProperties.getWriteBufferSize;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.readBigDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Page sink implements the insert support
 */
public class TileDBPageSink
        implements ConnectorPageSink
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();
    private static final Logger log = Logger.get(TileDBPageSink.class);
    private static final Map<Integer, Datatype> channelToDatatype = new HashMap<>();

    private Query query;
    private final Array array;
    private final Context ctx;

    private final List<TileDBColumnHandle> columnHandles;
    // ColumnOrder holds the `channel` number for a column
    // This is used specifically for fetching and storing dimensions in proper order for tiledb coordinates
    private final Map<String, Integer> columnOrder;

    private final TileDBOutputTableHandle table;
    private final int maxBufferSize; // Max Buffer

    /**
     * Initialize an instance of page sink preparing for inserts
     * @param handle table handler
     * @param tileDBClient client (for context)
     * @param session
     */
    public TileDBPageSink(TileDBOutputTableHandle handle, TileDBClient tileDBClient, ConnectorSession session)
    {
        try {
            ctx = tileDBClient.buildContext(session);
            // Set max write buffer size from session configuration parameter
            this.maxBufferSize = getWriteBufferSize(session);

            // Open the array in write mode
            array = new Array(ctx, handle.getURI(), QueryType.TILEDB_WRITE);
            // Create query object
            query = new Query(array, QueryType.TILEDB_WRITE);
            // All writes are unordered
            query.setLayout(Layout.TILEDB_UNORDERED);

            this.table = handle;
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_PAGE_SINK_ERROR, tileDBError);
        }

        columnHandles = handle.getColumnHandles();
        columnOrder = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            columnOrder.put(columnHandles.get(i).getColumnName(), i);
        }
    }

    /**
     * Reset query, as of TileDB 2.0 a query object should not be reused for writing unless in global order
     * @param buffers
     * @throws TileDBError
     */
    private void resetQuery(Map<String, Pair<NativeArray, NativeArray>> buffers) throws TileDBError
    {
        // Create query object
        query.close();
        query = new Query(array, QueryType.TILEDB_WRITE);
        // All writes are unordered
        query.setLayout(Layout.TILEDB_UNORDERED);
        resetBuffers(buffers);
    }

    /**
     * Reset query buffers by closing and re-allocating
     * @param buffers
     * @throws TileDBError
     */
    private void resetBuffers(Map<String, Pair<NativeArray, NativeArray>> buffers) throws TileDBError
    {
        for (Map.Entry<String, Pair<NativeArray, NativeArray>> bufferEntry : buffers.entrySet()) {
            NativeArray offsets = bufferEntry.getValue().getFirst();
            NativeArray values = bufferEntry.getValue().getSecond();
            if (offsets != null) {
                offsets.close();
            }
            if (values != null) {
                values.close();
            }
        }
        buffers.clear();
        query.resetBuffers();

        // Loop through each column
        for (int channel = 0; channel < columnHandles.size(); channel++) {
            // Datatype
            Datatype type = null;
            // Is column of variable length
            boolean isVariableLength = false;
            TileDBColumnHandle columnHandle = columnHandles.get(channel);
            String columnName = columnHandle.getColumnName();
            NativeArray values = null;
            NativeArray offsets = null;

            type = columnHandle.getColumnTileDBType();
            isVariableLength = columnHandle.getIsVariableLength();
            // If the attribute is variable length create offset and values arrays
            values = new NativeArray(ctx, maxBufferSize, type);
            if (isVariableLength) {
                offsets = new NativeArray(ctx, maxBufferSize, Datatype.TILEDB_UINT64);
            }
            buffers.put(columnName, new Pair<>(offsets, values));
        }
    }

    /**
     * appendPage adds the rows
     * @param page rows/columns to insert
     * @return Future not currently used, but could be for async writing
     */
    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            Map<String, Pair<Optional<Long>, Long>> bufferEffectiveSizes = new HashMap<>();
            Map<String, Pair<NativeArray, NativeArray>> buffers = new HashMap<>();
            initBufferEffectiveSizes(bufferEffectiveSizes);
            // Position is row, channel is column
            resetQuery(buffers);

            // Loop through each row for the column
            for (int position = 0; position < page.getPositionCount(); position++) {
                Map<String, Pair<Optional<Long>, Long>> previousBufferEffectiveSizes = bufferEffectiveSizes;
                try {
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        TileDBColumnHandle columnHandle = columnHandles.get(channel);
                        String columnName = columnHandle.getColumnName();

                        // Get the current effective size of the buffers
                        Pair<Optional<Long>, Long> bufferEffectiveSize = bufferEffectiveSizes.get(columnName);
                        int bufferPosition = toIntExact(bufferEffectiveSize.getSecond());
                        Optional<Long> offsetSize = bufferEffectiveSize.getFirst();
                        // If we have a dimension we need to set the position for the coordinate buffer based on dimension ordering
                        Pair<NativeArray, NativeArray> bufferPair = buffers.get(columnName);
                        // For variable length attributes we always start the position at the current max size.
                        if (columnHandle.getIsVariableLength()) {
                            bufferPair.getFirst().setItem(toIntExact(offsetSize.get()), bufferEffectiveSize.getSecond());
                            offsetSize = Optional.of(offsetSize.get() + 1);
                        }
                        // Add this value to the array
                        Long newBufferEffectiveSize = appendColumn(page, position, channel, bufferPair.getSecond(), bufferPosition);
                        bufferEffectiveSizes.put(columnName, new Pair<>(offsetSize, newBufferEffectiveSize));
                    }
                }
                catch (IndexOutOfBoundsException e) {
                    position--;
                    // Revert to last complete loop of buffer sizes, since we ran out during this iteration.
                    bufferEffectiveSizes = previousBufferEffectiveSizes;
                    // submitQuery
                    if (submitQuery(buffers, bufferEffectiveSizes) == QueryStatus.TILEDB_FAILED) {
                        throw new PrestoException(TILEDB_PAGE_SINK_ERROR, e);
                    }
                    resetQuery(buffers);
                    bufferEffectiveSizes.clear();
                    initBufferEffectiveSizes(bufferEffectiveSizes);
                }
            }

            // Submit query one last time for any remaining elements to insert
            submitQuery(buffers, bufferEffectiveSizes);

            // Free all buffers
            for (Map.Entry<String, Pair<NativeArray, NativeArray>> entry : buffers.entrySet()) {
                if (entry.getValue().getFirst() != null) {
                    entry.getValue().getFirst().close();
                }
                if (entry.getValue().getSecond() != null) {
                    entry.getValue().getSecond().close();
                }
            }
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_PAGE_SINK_ERROR, tileDBError);
        }

        return NOT_BLOCKED;
    }

    /**
     * Initialize the map holding the effective buffer sizes
     * @param bufferEffectiveSizes
     */
    private void initBufferEffectiveSizes(Map<String, Pair<Optional<Long>, Long>> bufferEffectiveSizes)
    {
        for (TileDBColumnHandle column : columnHandles) {
            if (column.getIsVariableLength()) {
                bufferEffectiveSizes.put(column.getColumnName(), new Pair<>(Optional.of(0L), 0L));
            }
            else {
                bufferEffectiveSizes.put(column.getColumnName(), new Pair<>(Optional.empty(), 0L));
            }
        }
    }

    /**
     * Submit a query to tiledb for writing
     * @param buffers Map of buffers to write
     * @param bufferEffectiveSizes Map of effective buffer sizes
     * @return QueryStatus
     * @throws TileDBError
     */
    private QueryStatus submitQuery(Map<String, Pair<NativeArray, NativeArray>> buffers, Map<String, Pair<Optional<Long>, Long>> bufferEffectiveSizes) throws TileDBError
    {
        // We have to keep track of if we created a new buffer or not and if we should clear it
        List<NativeArray> buffersToClear = new ArrayList<>();
        // We need to know how many elements are suppose to be in offset buffers. We can check this by seeing the effective size of the the coordinates buffer
        Pair<Optional<Long>, Long> sizes = bufferEffectiveSizes.values().stream().findFirst().get();
        long effectiveElementsInOffsetBuffers = sizes.getFirst().orElseGet(sizes::getSecond);

        // Loop through each buffer to set it on query object
        for (Map.Entry<String, Pair<NativeArray, NativeArray>> bufferEntry : buffers.entrySet()) {
            Datatype nativeType;
            String name = bufferEntry.getKey();

            if (array.getSchema().getDomain().hasDimension(name)) {
                nativeType = array.getSchema().getDomain().getDimension(name).getType();
            }
            else {
                nativeType = array.getSchema().getAttribute(name).getType();
            }

            NativeArray offsets = bufferEntry.getValue().getFirst();
            NativeArray values = bufferEntry.getValue().getSecond();
            // LastValuePosition holds the position of the last element in the buffer
            long effectiveElementInBuffer = bufferEffectiveSizes.get(bufferEntry.getKey()).getSecond();
            // If the buffer is larger than the last position we need to resize
            if (values.getSize() > effectiveElementInBuffer) {
                if (values.getJavaType().equals(String.class)) {
                    values = new NativeArray(ctx, new String((byte[]) values.toJavaArray(toIntExact(effectiveElementInBuffer))), values.getJavaType());
                }
                else {
                    values = new NativeArray(ctx, values.toJavaArray(toIntExact(effectiveElementInBuffer)), nativeType);
                }
                buffersToClear.add(values);
            }
            // If the offset buffer is not null then we are dealing with a variable length
            if (offsets != null) {
                // If the buffer is larger than the last position we need to resize
                if (offsets.getSize() > effectiveElementsInOffsetBuffers) {
                    offsets = new NativeArray(ctx, offsets.toJavaArray(toIntExact(effectiveElementsInOffsetBuffers)), Datatype.TILEDB_UINT64);
                    buffersToClear.add(offsets);
                }
                query.setBuffer(bufferEntry.getKey(), offsets, values);
            }
            else {
                query.setBuffer(bufferEntry.getKey(), values);
            }
        }
        // Set the coordinates and submit
        QueryStatus status = query.submit();
        // Very Important we must call reset buffers to remove the internal cache of the query object. This is java not core tiledb
        query.resetBuffers();
        for (NativeArray nativeArray : buffersToClear) {
            nativeArray.close();
        }
        return status;
    }

    /**
     * Append a column to appropriate buffer
     * @param page Page from presto containing data
     * @param position current row number
     * @param channel column index
     * @param columnBuffer NativeBuffer for column data
     * @param bufferPosition The current position of the buffer (where a write should start)
     * @return new effective buffer size after write
     * @throws TileDBError
     */
    private long appendColumn(Page page, int position, int channel, NativeArray columnBuffer, int bufferPosition) throws TileDBError
    {
        Block block = page.getBlock(channel);
        if (block.isNull(position)) {
            throw new TileDBError("Null values not allowed for insert. Error in table " + table.getTableName() + ", column " + columnHandles.get(channel).getColumnName() + ", row " + position);
        }

        String colName = columnHandles.get(channel).getColumnName();
        Datatype colType;

        if (channelToDatatype.containsKey(channel)) {
            colType = channelToDatatype.get(channel);
        }
        else {
            if (array.getSchema().getDomain().hasDimension(colName)) {
                colType = array.getSchema().getDomain().getDimension(colName).getType();
            }
            else {
                colType = array.getSchema().getAttribute(colName).getType();
            }
            channelToDatatype.put(channel, colType);
        }

        int size = 1;
        Type type = columnHandles.get(channel).getColumnType();

        // Only varchar and varbinary are supported for variable length attributes, so we only check these for additional size requirements
        if (isVarcharType(type) || isCharType(type)) {
            size = type.getSlice(block, position).toStringUtf8().length();
        }
        else if (VARBINARY.equals(type)) {
            size = type.getSlice(block, position).getBytes().length;
        }
        // Check to see if we need to re-allocate array,this only happens for variable length attributes
        if (bufferPosition + size >= columnBuffer.getSize()) {
            throw new IndexOutOfBoundsException("Buffer outside of allocated memory");
        }

        // Switch on type and add value to buffer
        if (BOOLEAN.equals(type)) {
            columnBuffer.setItem(bufferPosition, type.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            columnBuffer.setItem(bufferPosition, type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            columnBuffer.setItem(bufferPosition, toIntExact(type.getLong(block, position)));
        }
        else if (SMALLINT.equals(type)) {
            columnBuffer.setItem(bufferPosition, Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (TINYINT.equals(type)) {
            columnBuffer.setItem(bufferPosition, SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (DOUBLE.equals(type)) {
            columnBuffer.setItem(bufferPosition, type.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            columnBuffer.setItem(bufferPosition, intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (type instanceof DecimalType) {
            columnBuffer.setItem(bufferPosition, readBigDecimal((DecimalType) type, block, position).doubleValue());
        }
        else if (isVarcharType(type) || isCharType(type)) {
            columnBuffer.setItem(bufferPosition, type.getSlice(block, position).toStringUtf8());
        }
        else if (VARBINARY.equals(type)) {
            columnBuffer.setItem(bufferPosition, type.getSlice(block, position).getBytes());
        }
        else if (DATE.equals(type)) {
            long value;
            switch (colType) {
                case TILEDB_DATETIME_AS: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusNanos((long) ((type.getLong(block, position)) * 0.0001)).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_FS: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusNanos((long) ((type.getLong(block, position)) * 0.001)).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_PS: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusNanos((long) ((type.getLong(block, position)) * 0.01)).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_NS: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusNanos((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_US: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusNanos((type.getLong(block, position)) * 1000).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_DAY:
                case TILEDB_DATETIME_MS: {
                    value = type.getLong(block, position);
                    break;
                }
                case TILEDB_DATETIME_SEC: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusSeconds((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_MIN: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusMinutes((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_HR: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusHours((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_WEEK: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusWeeks((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_MONTH: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusMonths((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                case TILEDB_DATETIME_YEAR: {
                    value = new Timestamp(0).toInstant().atOffset(ZoneOffset.UTC).plusYears((type.getLong(block, position))).toInstant().toEpochMilli();
                    break;
                }
                default: {
                    throw new TileDBError("Type: " + colType + " is not supported");
                }
            }

            columnBuffer.setItem(bufferPosition, value);
        }
        else if (TIMESTAMP.equals(type)) {
            columnBuffer.setItem(bufferPosition, type.getLong(block, position));
        }
        else {
            throw new PrestoException(TILEDB_PAGE_SINK_ERROR, "Unsupported column type: " + type.getDisplayName());
        }

        return bufferPosition + size;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // finalize and close
        try {
            query.finalizeQuery();
            query.close();
            array.close();
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_PAGE_SINK_ERROR, tileDBError);
        }
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
        query.close();
        array.close();
    }
}
