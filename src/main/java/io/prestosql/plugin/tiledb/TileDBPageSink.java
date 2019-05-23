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

import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.Layout;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.QueryStatus;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.tiledb.java.api.Constants.TILEDB_COORDS;
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

    private final Query query;
    private final Array array;
    private final Context ctx;

    private final List<TileDBColumnHandle> columnHandles;
    // ColumnOrder holds the `channel` number for a column
    // This is used specifically for fetching and storing dimensions in proper order for tiledb coordinates
    private final Map<String, Integer> columnOrder;

    private final Map<String, Integer> dimensionOrder;
    private final List<String> dimensionNameOrder;
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

            // For coordinates we need to have dimensions in their proper order, here we will get the ordering
            dimensionOrder = new HashMap<>();
            dimensionNameOrder = new ArrayList<>();
            int i = 0;
            try (ArraySchema arraySchema = array.getSchema(); Domain domain = arraySchema.getDomain()) {
                for (Dimension dimension : domain.getDimensions()) {
                    dimensionOrder.put(dimension.getName(), i++);
                    dimensionNameOrder.add(dimension.getName());
                    dimension.close();
                }
            }

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
            boolean isDimension = dimensionOrder.containsKey(columnName);
            NativeArray values = null;
            NativeArray offsets = null;

            // If column is not a dimension check to see if its an attribute
            if (!isDimension) {
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
        // Get list of dimensions
        try (ArraySchema arraySchema = array.getSchema(); Domain domain = arraySchema.getDomain()) {
            List<Dimension> dimensions = domain.getDimensions();
            Datatype dimType = dimensions.get(0).getType();
            NativeArray coordinates = new NativeArray(ctx, maxBufferSize * dimensions.size(), dimType);
            buffers.put(TILEDB_COORDS, new Pair<>(null, coordinates));
            for (Dimension d : dimensions) {
                d.close();
            }
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
            Map<String, Long> bufferEffectiveSizes = new HashMap<>();
            Map<String, Pair<NativeArray, NativeArray>> buffers = new HashMap<>();
            initBufferEffectiveSizes(bufferEffectiveSizes);
            // Position is row, channel is column
            resetBuffers(buffers);

            // Loop through each row for the column
            for (int position = 0; position < page.getPositionCount(); position++) {
                Map<String, Long> previousBufferEffectiveSizes = bufferEffectiveSizes;
                try {
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        TileDBColumnHandle columnHandle = columnHandles.get(channel);
                        String columnName = columnHandle.getColumnName();
                        // If the current column is a dimension we will skip, as all dimensions are handled at the end of the row
                        if (dimensionOrder.containsKey(columnName)) {
                            continue;
                        }
                        // Get the current effective size of the buffers
                        Long bufferEffectiveSize = bufferEffectiveSizes.get(columnName);
                        int bufferPosition = toIntExact(bufferEffectiveSize);
                        // If we have a dimension we need to set the position for the coordinate buffer based on dimension ordering
                        Pair<NativeArray, NativeArray> bufferPair = buffers.get(columnName);
                        // For variable length attributes we always start the position at the current max size.
                        if (columnHandle.getIsVariableLength()) {
                            bufferPair.getFirst().setItem(position, bufferEffectiveSize);
                        }
                        // Add this value to the array
                        Long newBufferEffectiveSize = appendColumn(page, position, channel, bufferPair.getSecond(), bufferPosition);
                        bufferEffectiveSizes.put(columnName, newBufferEffectiveSize);
                    }

                    // Add dimension in proper order to coordinates
                    for (String dimension : dimensionNameOrder) {
                        int channel = columnOrder.get(dimension);
                        Long bufferEffectiveSize = bufferEffectiveSizes.get(TILEDB_COORDS);
                        Pair<NativeArray, NativeArray> bufferPair = buffers.get(TILEDB_COORDS);
                        int bufferPosition = toIntExact(bufferEffectiveSize);
                        Long newBufferEffectiveSize = appendColumn(page, position, channel, bufferPair.getSecond(), bufferPosition);
                        bufferEffectiveSizes.put(TILEDB_COORDS, newBufferEffectiveSize);
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
                    resetBuffers(buffers);
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
    private void initBufferEffectiveSizes(Map<String, Long> bufferEffectiveSizes)
    {
        for (String columnName : columnHandles.stream().map(TileDBColumnHandle::getColumnName).collect(Collectors.toList())) {
            if (!dimensionOrder.containsKey(columnName)) {
                bufferEffectiveSizes.put(columnName, 0L);
            }
        }
        bufferEffectiveSizes.put(TILEDB_COORDS, 0L);
    }

    /**
     * Submit a query to tiledb for writing
     * @param buffers Map of buffers to write
     * @param bufferEffectiveSizes Map of effective buffer sizes
     * @return QueryStatus
     * @throws TileDBError
     */
    private QueryStatus submitQuery(Map<String, Pair<NativeArray, NativeArray>> buffers, Map<String, Long> bufferEffectiveSizes) throws TileDBError
    {
        // We have to keep track of if we created a new buffer or not and if we should clear it
        List<NativeArray> buffersToClear = new ArrayList<>();
        // We need to know how many elements are suppose to be in offset buffers. We can check this by seeing the effective size of the the coordinates buffer
        long effectiveElementsInOffsetBuffers = bufferEffectiveSizes.get(TILEDB_COORDS) / dimensionOrder.size();

        // Loop through each buffer to set it on query object
        for (Map.Entry<String, Pair<NativeArray, NativeArray>> bufferEntry : buffers.entrySet()) {
            NativeArray offsets = bufferEntry.getValue().getFirst();
            NativeArray values = bufferEntry.getValue().getSecond();
            // LastValuePosition holds the position of the last element in the buffer
            long effectiveElementInBuffer;
            // Handle coordinate buffer
            if (bufferEntry.getKey().equals(TILEDB_COORDS)) {
                effectiveElementInBuffer = effectiveElementsInOffsetBuffers * dimensionOrder.size();
            }
            else {
                effectiveElementInBuffer = bufferEffectiveSizes.get(bufferEntry.getKey());
            }
            // If the buffer is larger than the last position we need to resize
            if (values.getSize() > effectiveElementInBuffer) {
                if (values.getJavaType().equals(String.class)) {
                    values = new NativeArray(ctx, new String((byte[]) values.toJavaArray(toIntExact(effectiveElementInBuffer))), values.getJavaType());
                }
                else {
                    values = new NativeArray(ctx, values.toJavaArray(toIntExact(effectiveElementInBuffer)), values.getJavaType());
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

        int size = 1;
        Type type = columnHandles.get(channel).getColumnType();

        // Only varchar and varbinary are supported for variable length attributes, so we only check these for additional size requirements
        if (isVarcharType(type) || isCharType(type)) {
            size = type.getSlice(block, position).toStringUtf8().length();
        }
        else if (VARBINARY.equals(type)) {
            size = type.getSlice(block, position).getBytes().length;
        }
        else if (DATE.equals(type)) {
            size = DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(type.getLong(block, position))).length();
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
        else if (DATE.equals(type)) { // NOTE: this is not used because we make all date columns a varchar type
            columnBuffer.setItem(bufferPosition, DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(type.getLong(block, position))));
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
