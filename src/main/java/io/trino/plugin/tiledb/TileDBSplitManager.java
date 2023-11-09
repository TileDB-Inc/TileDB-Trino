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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.EncryptionType;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.QueryType;
import io.tiledb.java.api.TileDBError;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.inject.Inject;
import org.apache.commons.beanutils.ConvertUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.tiledb.java.api.QueryType.TILEDB_READ;
import static io.trino.plugin.tiledb.TileDBErrorCode.TILEDB_RECORD_SET_ERROR;
import static io.trino.plugin.tiledb.TileDBErrorCode.TILEDB_SPLIT_MANAGER_ERROR;
import static io.trino.plugin.tiledb.TileDBSessionProperties.getEncryptionKey;
import static io.trino.plugin.tiledb.TileDBSessionProperties.getSplitOnlyPredicates;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

/**
 * TileDBSplitManager is responsible for managing and manipulating `splits`. Currently tiledb does not implement splits.
 */
public class TileDBSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(TileDBSplitManager.class);
    private final String connectorId;
    private final TileDBClient tileDBClient;
    private final NodeManager nodeManager;

    private final TileDBMetadata tileDBMetadata;

    @Inject
    public TileDBSplitManager(TileDBConnectorId connectorId, TileDBClient tileDBClient, NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.tileDBClient = requireNonNull(tileDBClient, "client is null");
        this.tileDBMetadata = new TileDBMetadata(connectorId, tileDBClient);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint)
    {
        TileDBTableHandle tableHandle = (TileDBTableHandle) table;

        try {
            Array array;
            String key = getEncryptionKey(session);
            if (key == null) {
                array = new Array(tileDBClient.buildContext(session, null, null), tableHandle.getURI());
            }
            else {
                array = new Array(tileDBClient.buildContext(session, EncryptionType.TILEDB_AES_256_GCM, key),
                        tableHandle.getURI(), QueryType.TILEDB_READ);
            }
            int numSplits = TileDBSessionProperties.getSplits(session);
            if (numSplits == -1) {
                numSplits = nodeManager.getWorkerNodes().size();
            }

            Object[] domainAndColumnHandlesSize = getTupleDomain(session, tableHandle, constraint);
            TupleDomain<ColumnHandle> tupleDomain = (TupleDomain<ColumnHandle>) domainAndColumnHandlesSize[0];
            int dimensionCount = ((Set<ColumnHandle>) domainAndColumnHandlesSize[1]).size();

            List<TupleDomain<ColumnHandle>> domainRangeSplits = splitTupleDomainOnRanges(tupleDomain, numSplits, TileDBSessionProperties.getSplitOnlyPredicates(session), array.nonEmptyDomain(), dimensionCount);

            List<TileDBSplit> splits = domainRangeSplits.stream().map(
                    tuple -> new TileDBSplit(
                            connectorId,
                            tableHandle.getSchemaName(),
                            tableHandle.getTableName(),
                            tuple)).collect(Collectors.toList());

            return new FixedSplitSource(splits);
        }
        catch (TileDBError tileDBError) {
            throw new TrinoException(TILEDB_SPLIT_MANAGER_ERROR, tileDBError);
        }
    }

    /**
     * Returns the tuple domain and the column handles
     *
     * @param session The connector session
     * @param tableHandle The table handle
     * @param constraint The constraint
     * @return an Object[] array with a size of 2 to return both objects
     */
    private Object[] getTupleDomain(ConnectorSession session, TileDBTableHandle tableHandle, Constraint constraint)
    {
        Object[] result = new Object[2];

        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary();

        Map<String, ColumnHandle> columns = tileDBMetadata.getColumnHandles(session, tableHandle);

        Set<ColumnHandle> dimensionHandles = columns.values().stream()
                .filter(e -> ((TileDBColumnHandle) e).getIsDimension())
                .collect(Collectors.toSet());

        // The only enforceable constraints are ones for dimension columns
        Map<ColumnHandle, Domain> enforceableDimensionDomains = new HashMap<>(Maps.filterKeys(effectivePredicate.getDomains().get(), Predicates.in(dimensionHandles)));

        if (!getSplitOnlyPredicates(session)) {
            try {
                Array array;
                String key = getEncryptionKey(session);
                if (key == null) {
                    array = new Array(tileDBClient.buildContext(session, null, null), tableHandle.getURI(), TILEDB_READ);
                }
                else {
                    array = new Array(tileDBClient.buildContext(session, EncryptionType.TILEDB_AES_256_GCM, key),
                            tableHandle.getURI(), TILEDB_READ);
                }

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
                            if (nonEmptyMin == null || nonEmptyMax == null || nonEmptyMin.equals("") || nonEmptyMax.equals("")) {
                                continue;
                            }

                            Range range;
                            if (REAL.equals(type)) {
                                range = Range.range(type, ((Integer) floatToRawIntBits((Float) nonEmptyMin)).longValue(), true,
                                        ((Integer) floatToRawIntBits((Float) nonEmptyMax)).longValue(), true);
                            }
                            else if (type instanceof VarcharType) {
                                range = Range.range(type, utf8Slice(nonEmptyMin.toString()), true,
                                        utf8Slice(nonEmptyMax.toString()), true);
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
                array.close();
            }
            catch (TileDBError tileDBError) {
                throw new TrinoException(TILEDB_RECORD_SET_ERROR, tileDBError);
            }
        }

        TupleDomain<ColumnHandle> enforceableTupleDomain = TupleDomain.withColumnDomains(enforceableDimensionDomains);
        result[0] = enforceableTupleDomain;
        result[1] = dimensionHandles;
        return result;
    }

    /**
     * Split tuple domain if there are multiple ranges
     *
     * @param tupleDomain the tuple domain
     * @param splitOnlyPredicates the split predicates
     * @param nonEmptyDomains the non-empty domains
     * @param dimensionCount the dimension count
     * @return tuple domains after split
     */
    private List<TupleDomain<ColumnHandle>> splitTupleDomainOnRanges(TupleDomain<ColumnHandle> tupleDomain, int splits, boolean splitOnlyPredicates, HashMap<String, Pair> nonEmptyDomains, int dimensionCount)
    {
        List<Pair<ColumnHandle, List<Domain>>> domainList = new ArrayList<>();
        // Loop through each column handle's domain to see if there are ranges to split

        int totalSplitsByRanges = tupleDomain.getDomains().get().entrySet().stream()
                .filter(e -> ((TileDBColumnHandle) e.getKey()).getIsDimension())
                .mapToInt(i -> i.getValue().getValues().getRanges().getRangeCount())
                .sum();

        if (splitOnlyPredicates) {
            dimensionCount = ((Long) tupleDomain.getDomains().get().entrySet().stream()
                    .filter(e -> ((TileDBColumnHandle) e.getKey()).getIsDimension()).count()).intValue();
        }
        Long remainingSplitsPerDimension = 0L;
        int remainingSplits = splits - totalSplitsByRanges + dimensionCount;
        if (remainingSplits >= dimensionCount) {
            if (remainingSplits >= dimensionCount * dimensionCount) {
                remainingSplitsPerDimension = Math.round(Math.pow((remainingSplits), 1.0 / dimensionCount));
            }
            else {
                remainingSplitsPerDimension = 2L;
            }
        }

        for (Map.Entry<ColumnHandle, Domain> domainEntry : tupleDomain.getDomains().get().entrySet()) {
            TileDBColumnHandle columnHandle = (TileDBColumnHandle) domainEntry.getKey();
            // If the column is a dimension we look to produce new tupleDomains from each range so these can run in parallel
            Domain domain = domainEntry.getValue();
            if (columnHandle.getIsDimension()) {
                Pair nonEmptyDomain = nonEmptyDomains.get(columnHandle.getColumnName());
                List<Range> ranges = domain.getValues().getRanges().getOrderedRanges();
                List<Domain> columnDomains = new ArrayList<>();
                // Add each in dependent range to the arraylist
                for (Range range : ranges) {
                    // If there are splits remaining then we should split each dimension by the value.
                    if (remainingSplits > 0 && remainingSplitsPerDimension > 1) {
                        for (Range splitRange : splitRange(range, remainingSplitsPerDimension.intValue(), nonEmptyDomain)) {
                            columnDomains.add(Domain.create(ValueSet.ofRanges(splitRange), domain.isNullAllowed()));
                            remainingSplits--;
                        }
                    }
                    else {
                        columnDomains.add(Domain.create(ValueSet.ofRanges(range), domain.isNullAllowed()));
                    }
                }
                domainList.add(new Pair<>(domainEntry.getKey(), columnDomains));
            }
            // For attributes we don't need to split any ranges just include the entire domain predicates
            else {
                domainList.add(new Pair<>(domainEntry.getKey(), ImmutableList.of(domain)));
            }
        }
        List<Map<ColumnHandle, Domain>> intermediateResults = new ArrayList<>();
        List<TupleDomain<ColumnHandle>> results = new ArrayList<>();

        // Create combination for all tuple domains
        generateCombinationTupleDomains(domainList, intermediateResults, 0, new HashMap<>());
        for (Map<ColumnHandle, Domain> domain : intermediateResults) {
            results.add(TupleDomain.withColumnDomains(domain));
        }

        return results;
    }

    /**
     * Function to generate each tuple domain combination based on a list of domains for each column handle
     *
     * @param lists the lists of domains
     * @param result the result
     * @param depth the depth
     * @param current the current domain
     */
    private void generateCombinationTupleDomains(List<Pair<ColumnHandle, List<Domain>>> lists, List<Map<ColumnHandle, Domain>> result, int depth, Map<ColumnHandle, Domain> current)
    {
        if (depth == lists.size()) {
            result.add(current);
            return;
        }

        // Get the domain list for the current depth
        Pair<ColumnHandle, List<Domain>> domainLists = lists.get(depth);
        for (int i = 0; i < domainLists.getSecond().size(); ++i) {
            current.put(domainLists.getFirst(), domainLists.getSecond().get(i));
            // Copy the current tuple mapping since java uses references to maps
            Map<ColumnHandle, Domain> currentCopy = current.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
            generateCombinationTupleDomains(lists, result, depth + 1, currentCopy);
        }
    }

    /**
     * Split a range into N buckets. Currently only ranges of type long can be split with this naive algorithm.
     *
     * @param range to split
     * @param buckets number
     * @param nonEmptyDomain The none empty domain, used if there the range is one sides, i.e. >= X we change to >= X AND less than MaxNonEmpty
     * @return List of ranges
     */
    private List<Range> splitRange(Range range, int buckets, Pair nonEmptyDomain)
    {
        List<Range> ranges = new ArrayList<>();
        if (range.isSingleValue()) {
            return ImmutableList.of(range);
        }
        boolean minFromNonEmptyDomain = true;
        boolean maxFromNonEmptyDomain = true;
        // Number of buckets is 1 more thank number of splits (i.e. split 1 time into two buckets)
        // Only long dimensions can be split with naive algorithm
        if (
                !REAL.equals(range.getType()) &&
                nonEmptyDomain != null &&
                nonEmptyDomain.getFirst() != null &&
                range.getType().getJavaType() == long.class) {
            long min = (Long) ConvertUtils.convert(nonEmptyDomain.getFirst(), Long.class);
            if (!range.isLowUnbounded()) {
                min = (Long) range.getLowBoundedValue();
                minFromNonEmptyDomain = false;
            }

            long max = (Long) ConvertUtils.convert(nonEmptyDomain.getSecond(), Long.class);
            if (!range.isHighUnbounded()) {
                max = (Long) range.getHighBoundedValue();
                maxFromNonEmptyDomain = false;
            }

            // If min is greater than max, then this range cannot be split, as it returns zero records
            // This can happen if the query has a selection that puts an upper bound that is less than the
            // low bound, for example, SELECT * FROM ORDERS WHERE orderkey < 0
            if (min > max) {
                return ranges;
            }

            //it is possible that there are more buckets than the size of the range. In these cases we should use less buckets instead of all.
            buckets = (int) Math.max(((max - min) % buckets), 1);

            long rangeLength = (max - min) / buckets;
            long leftOvers = (max - min) % buckets;

            long low = min;
            for (int i = 0; i < buckets; i++) {
                // We want to set the high of the split range to be the low value of the range + the length - 1
                long high = low + rangeLength - 1;

                // Handle base case where range length is 1, so we don't need to substract one to account for inclusiveness
                boolean lowerInclusive = true;
                boolean highInclusive = true;
                // If this is the first split we need to set the bound to the same as the range lower bound
                if (i == 0 && !minFromNonEmptyDomain) {
                    lowerInclusive = range.isLowInclusive();
                }

                // If this is the last split we need to set the bond to the same as the range upper bound
                // Also make sure we don't leave any values out by setting the high to the max of the range
                if (i == buckets - 1) {
                    if (!maxFromNonEmptyDomain) {
                        highInclusive = range.isHighInclusive();
                    }
                    high = max;
                }
                // If this is not the last split we should spread out any leftOver values
                else if (leftOvers > 0) {
                    // Add one
                    high += 1;
                    leftOvers--;
                }
                // Only set the range if the values are not equal or if the low
                // and high are the bounds must also be the same
                if (low > high) {
                    LOG.warn("Low > high while setting ranges.");
                    return ranges;
                }

                if (low != high || lowerInclusive == highInclusive) {
                    ranges.add(range(
                            range.getType(),
                            low,
                            lowerInclusive,
                            high,
                            highInclusive));
                }
                // Set the low value to the high+1 for the next range split
                low = high + 1;
            }
        }
        else {
            ranges.add(range);
        }

        return ranges;
    }
}
