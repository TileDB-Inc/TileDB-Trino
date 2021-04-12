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
package com.facebook.presto.plugin.tiledb.util;

import io.tiledb.java.api.BitShuffleFilter;
import io.tiledb.java.api.BitWidthReductionFilter;
import io.tiledb.java.api.ByteShuffleFilter;
import io.tiledb.java.api.Bzip2Filter;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.DoubleDeltaFilter;
import io.tiledb.java.api.Filter;
import io.tiledb.java.api.FilterList;
import io.tiledb.java.api.GzipFilter;
import io.tiledb.java.api.LZ4Filter;
import io.tiledb.java.api.NoneFilter;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.ZstdFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util
{
    private Util() {}

    /**
     * The String Partitioner
     */
    private static final StringPartitioner stringPartitioner = new StringPartitioner();

    public static Dimension toDimension(Context localCtx, String dimName, Datatype type, Domain domain, Long extent,
                                        Long lowerBound, Long upperBound) throws TileDBError
    {
        Class classType = type.javaClass();
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
                return new Dimension(localCtx, dimName, classType, new Pair(lowerBound.byteValue(), upperBound.byteValue()), extent.byteValue());
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
                return new Dimension(localCtx, dimName, classType, new Pair(lowerBound.shortValue(), upperBound.shortValue()), extent.shortValue());
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
                return new Dimension(localCtx, dimName, classType, new Pair(lowerBound.intValue(), upperBound.intValue()), extent.intValue());
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
                return new Dimension(localCtx, dimName, type, new Pair(lowerBound, upperBound), extent);
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
                return new Dimension(localCtx, dimName, classType, new Pair(lowerBound.floatValue(), upperBound.floatValue()), extent.floatValue());
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
                return new Dimension(localCtx, dimName, classType, new Pair(lowerBound.doubleValue(), upperBound.doubleValue()), extent.doubleValue());
            case TILEDB_STRING_ASCII:
                return new Dimension(localCtx, dimName, type, null, null);
            default:
                throw new TileDBError(String.format("Invalid dimension datatype %s, must be one of [TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE]", type.toString()));
        }
    }

    /**
     * Parses a comma-separated filter list and returns a list with key-value pairs,
     * where the key is the filter name (e.g. bzip2) and the value the filter's value
     * (e.g. -1)
     * @param csvList
     * @return
     * @throws IllegalArgumentException
     */
    public static Optional<List<Pair<String, Integer>>> tryParseFilterList(String csvList)
            throws IllegalArgumentException
    {
        // filter lists are in the form "(filter, option), (filter, option), etc.")
        List<Pair<String, Integer>> filterResults = new ArrayList<>();
        // String[] splitVals = csvList.split("\\s*,\\s*");
        Pattern filterListRegex = Pattern.compile("\\((.*?)\\)");
        Matcher filterListMatcher = filterListRegex.matcher(csvList);
        while (filterListMatcher.find()) {
            String filterString = filterListMatcher.group(1);
            String[] filterPair = filterString.split("\\s*,\\s*");
            // remove parens
            String filterName = filterPair[0];
            List<String> validFilters = Arrays.asList(new String[]{"GZIP", "ZSTD", "LZ4", "RLE", "BZIP2",
                    "DOUBLE_DELTA", "BIT_WIDTH_REDUCTION", "BITSHUFFLE", "BYTESHUFFLE", "POSITIVE_DELTA"});

            if (!validFilters.contains(filterName.toUpperCase())) {
                throw new IllegalArgumentException("Unknown TileDB filter string value: " + filterName);
            }
            Integer filterOption = -1;
            if (filterPair.length == 2) {
                // remove parens
                String filterOptionStr = filterPair[1];
                try {
                    filterOption = Integer.parseInt(filterOptionStr);
                }
                catch (NumberFormatException err) {
                    throw new IllegalArgumentException(
                            "Cannot parse filter option value for " + filterName + ": " + filterOptionStr);
                }
            }
            filterResults.add(new Pair<>(filterName, filterOption));
        }
        if (filterResults.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(filterResults);
    }

    /**
     * Returns a TileDB FilterList
     * @param ctx The context
     * @param filterListDesc The filter pairs list extracted with the tryParseFilterList method
     * @return The FilterList instance
     * @throws TileDBError
     */
    public static FilterList createTileDBFilterList(
            Context ctx, List<Pair<String, Integer>> filterListDesc) throws TileDBError
    {
        FilterList filterList = new FilterList(ctx);
        try {
            for (Pair<String, Integer> filterDesc : filterListDesc) {
                String filterName = filterDesc.getFirst();
                Integer filterOption = filterDesc.getSecond();
                if (filterName.equalsIgnoreCase("NONE")) {
                    try (Filter filter = new NoneFilter(ctx)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("GZIP")) {
                    try (Filter filter = new GzipFilter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("ZSTD")) {
                    try (Filter filter = new ZstdFilter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("LZ4")) {
                    try (Filter filter = new LZ4Filter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("RLE")) {
                    try (Filter filter = new LZ4Filter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("BZIP2")) {
                    try (Filter filter = new Bzip2Filter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("DOUBLE_DELTA")) {
                    try (Filter filter = new DoubleDeltaFilter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("BIT_WIDTH_REDUCTION")) {
                    try (Filter filter = new BitWidthReductionFilter(ctx, filterOption)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("BITSHUFFLE")) {
                    try (Filter filter = new BitShuffleFilter(ctx)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("BYTESHUFFLE")) {
                    try (Filter filter = new ByteShuffleFilter(ctx)) {
                        filterList.addFilter(filter);
                    }
                }
                else if (filterName.equalsIgnoreCase("POSITIVE_DELTA")) {
                    try (Filter filter = new ByteShuffleFilter(ctx)) {
                        filterList.addFilter(filter);
                    }
                }
            }
        }
        catch (TileDBError err) {
            filterList.close();
            throw err;
        }
        return filterList;
    }

    /**
     * Returns v + eps, where eps is the smallest value for the datatype such that v + eps > v.
     */
    public static Object addEpsilon(Object value, Datatype type) throws TileDBError
    {
        switch (type) {
            case TILEDB_CHAR:
            case TILEDB_INT8:
                return ((byte) value) < Byte.MAX_VALUE ? ((byte) value + 1) : value;
            case TILEDB_INT16:
                return ((short) value) < Short.MAX_VALUE ? ((short) value + 1) : value;
            case TILEDB_INT32:
                return ((int) value) < Integer.MAX_VALUE ? ((int) value + 1) : value;
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
                return ((long) value) < Long.MAX_VALUE ? ((long) value + 1) : value;
            case TILEDB_UINT8:
                return ((short) value) < ((short) Byte.MAX_VALUE + 1) ? ((short) value + 1) : value;
            case TILEDB_UINT16:
                return ((int) value) < ((int) Short.MAX_VALUE + 1) ? ((int) value + 1) : value;
            case TILEDB_UINT32:
                return ((long) value) < ((long) Integer.MAX_VALUE + 1) ? ((long) value + 1) : value;
            case TILEDB_UINT64:
                return ((long) value) < ((long) Integer.MAX_VALUE + 1) ? ((long) value + 1) : value;
            case TILEDB_FLOAT32:
                return ((float) value) < Float.MAX_VALUE ? Math.nextUp((float) value) : value;
            case TILEDB_FLOAT64:
                return ((double) value) < Double.MAX_VALUE ? Math.nextUp((double) value) : value;
            case TILEDB_STRING_ASCII:
                return stringPartitioner.nextStr((String) value);
            default:
                throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
        }
    }

    /**
     * Returns v - eps, where eps is the smallest value for the datatype such that v - eps < v.
     */
    public static Object subtractEpsilon(Object value, Datatype type) throws TileDBError
    {
        switch (type) {
            case TILEDB_CHAR:
            case TILEDB_INT8:
                return ((byte) value) > Byte.MIN_VALUE ? ((byte) value - 1) : value;
            case TILEDB_INT16:
                return ((short) value) > Short.MIN_VALUE ? ((short) value - 1) : value;
            case TILEDB_INT32:
                return ((int) value) > Integer.MIN_VALUE ? ((int) value - 1) : value;
            case TILEDB_INT64:
                return ((long) value) > Long.MIN_VALUE ? ((long) value - 1) : value;
            case TILEDB_UINT8:
                return ((short) value) > ((short) Byte.MIN_VALUE - 1) ? ((short) value - 1) : value;
            case TILEDB_UINT16:
                return ((int) value) > ((int) Short.MIN_VALUE - 1) ? ((int) value - 1) : value;
            case TILEDB_UINT32:
                return ((long) value) > ((long) Integer.MIN_VALUE - 1) ? ((long) value - 1) : value;
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
            case TILEDB_UINT64:
                return ((long) value) > ((long) Integer.MIN_VALUE - 1) ? ((long) value - 1) : value;
            case TILEDB_FLOAT32:
                return ((float) value) > Float.MIN_VALUE ? Math.nextDown((float) value) : value;
            case TILEDB_FLOAT64:
                return ((double) value) > Double.MIN_VALUE ? Math.nextDown((double) value) : value;
            case TILEDB_STRING_ASCII:
                return stringPartitioner.previousStr((String) value);
            default:
                throw new TileDBError("Unsupported TileDB Datatype enum: " + type);
        }
    }
}
