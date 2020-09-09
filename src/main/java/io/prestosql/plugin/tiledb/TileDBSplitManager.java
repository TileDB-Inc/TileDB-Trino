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
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.TileDBError;
import org.apache.commons.beanutils.ConvertUtils;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.plugin.tiledb.TileDBErrorCode.TILEDB_SPLIT_MANAGER_ERROR;
import static io.prestosql.spi.predicate.Utils.nativeValueToBlock;
import static io.prestosql.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;

/**
 * TileDBSplitManager is responsible for managing and manipulating `splits`. Currently tiledb does not implement splits.
 */
public class TileDBSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final TileDBClient tileDBClient;
    private TileDBTableLayoutHandle layoutHandle;
    private final NodeManager nodeManager;

    @Inject
    public TileDBSplitManager(TileDBConnectorId connectorId, TileDBClient tileDBClient, NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.tileDBClient = requireNonNull(tileDBClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        this.layoutHandle = (TileDBTableLayoutHandle) layout;

        TileDBTableHandle tableHandle = layoutHandle.getTable();

        try (Array array = new Array(tileDBClient.buildContext(session), tableHandle.getURI())) {
            int numSplits = TileDBSessionProperties.getSplits(session);
            if (numSplits == -1) {
                numSplits = nodeManager.getWorkerNodes().size();
            }
            List<TupleDomain<ColumnHandle>> domainRangeSplits = splitTupleDomainOnRanges(layoutHandle.getTupleDomain(), numSplits, TileDBSessionProperties.getSplitOnlyPredicates(session), array.nonEmptyDomain());

            List<TileDBSplit> splits = domainRangeSplits.stream().map(
                    tuple -> new TileDBSplit(
                            connectorId,
                            tableHandle.getSchemaName(),
                            tableHandle.getTableName(),
                            tuple)).collect(Collectors.toList());

            return new FixedSplitSource(splits);
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_SPLIT_MANAGER_ERROR, tileDBError);
        }
    }

        /**
     * Split tuple domain if there are multiple ranges
     * @param tupleDomain
     * @param splitOnlyPredicates
     * @param nonEmptyDomains
         * @return
     */
    private List<TupleDomain<ColumnHandle>> splitTupleDomainOnRanges(TupleDomain<ColumnHandle> tupleDomain, int splits, boolean splitOnlyPredicates, HashMap<String, Pair> nonEmptyDomains)
    {
        List<Pair<ColumnHandle, List<Domain>>> domainList = new ArrayList<>();
        // Loop through each column handle's domain to see if there are ranges to split

        int totalSplitsByRanges = tupleDomain.getDomains().get().entrySet().stream()
                .filter(e -> ((TileDBColumnHandle) e.getKey()).getIsDimension())
                .mapToInt(i -> i.getValue().getValues().getRanges().getRangeCount())
                .sum();

        int dimensionCount = this.layoutHandle.getDimensionColumnHandles().size();
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
        GenerateCombinationTupleDomains(domainList, intermediateResults, 0, new HashMap<>());
        for (Map<ColumnHandle, Domain> domain : intermediateResults) {
            results.add(TupleDomain.withColumnDomains(domain));
        }

        return results;
    }

    /**
     * Function to generate each tuple domain combination based on a list of domains for each column handle
     * @param lists
     * @param result
     * @param depth
     * @param current
     */
    private void GenerateCombinationTupleDomains(List<Pair<ColumnHandle, List<Domain>>> lists, List<Map<ColumnHandle, Domain>> result, int depth, Map<ColumnHandle, Domain> current)
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
            GenerateCombinationTupleDomains(lists, result, depth + 1, currentCopy);
        }
    }

    /**
     * Split a range into N buckets. Currently only ranges of type long can be split with this naive algorithm.
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
        if (!REAL.equals(range.getType()) && range.getType().getJavaType() == long.class) {
            long min = (Long) ConvertUtils.convert(nonEmptyDomain.getFirst(), Long.class);
            if (range.getLow().getValueBlock().isPresent()) {
                min = (Long) range.getLow().getValue();
                minFromNonEmptyDomain = false;
            }

            long max = (Long) ConvertUtils.convert(nonEmptyDomain.getSecond(), Long.class);
            if (range.getHigh().getValueBlock().isPresent()) {
                max = (Long) range.getHigh().getValue();
                maxFromNonEmptyDomain = false;
            }

            // If min is less than max, then this range cannot be split, as it returns zero records
            // This can happen if the query has a selection that puts an upper bound that is less than the
            // low bound, for example, SELECT * FROM ORDERS WHERE orderkey < 0
            if (min > max) {
                return ranges;
            }

            long rangeLength = (max - min) / buckets;
            long leftOvers = (max - min) % buckets;

            long low = min;
            for (int i = 0; i < buckets; i++) {
                Marker.Bound lowBound = Marker.Bound.EXACTLY;
                Marker.Bound upperBound = Marker.Bound.EXACTLY;
                // We want to set the high of the split range to be the low value of the range + the length - 1
                long high = low + rangeLength - 1;
                // Handle base case where range length is 1, so we don't need to substract one to account for inclusiveness

                // If this is the first split we need to set the bound to the same as the range lower bound
                if (i == 0 && !minFromNonEmptyDomain) {
                    lowBound = range.getLow().getBound();
                }

                // If this is the last split we need to set the bond to the same as the range upper bound
                // Also make sure we don't leave any values out by setting the high to the max of the range
                if (i == buckets - 1) {
                    if (!maxFromNonEmptyDomain) {
                        upperBound = range.getHigh().getBound();
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
                if (low != high || lowBound == upperBound) {
                    ranges.add(new Range(
                            new Marker(range.getType(), Optional.of(nativeValueToBlock(range.getType(), low)), lowBound),
                            new Marker(range.getType(), Optional.of(nativeValueToBlock(range.getType(), high)), upperBound)));
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
