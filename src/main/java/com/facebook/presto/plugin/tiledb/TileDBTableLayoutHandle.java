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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * TileDBTableLayoutHandle implements the thin interface for a connector table handler. The main purpose is to
 * hold the table handler instance and the tupleDomain which contains the constraints for each column
 */
public class TileDBTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final TileDBTableHandle table;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final Set<ColumnHandle> dimensionColumnHandles;

    @JsonCreator
    public TileDBTableLayoutHandle(
            @JsonProperty("table") TileDBTableHandle table,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> domain,
            @JsonProperty("dimensionColumnHandles") Set<ColumnHandle> dimensionColumnHandles)
    {
        this.table = table;
        this.tupleDomain = requireNonNull(domain, "tupleDomain is null");
        this.dimensionColumnHandles = requireNonNull(dimensionColumnHandles, "dimensionColumnHandles is null");
    }

    @JsonProperty
    public TileDBTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    @JsonProperty
    public Set<ColumnHandle> getDimensionColumnHandles()
    {
        return dimensionColumnHandles;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TileDBTableLayoutHandle that = (TileDBTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tupleDomain);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
