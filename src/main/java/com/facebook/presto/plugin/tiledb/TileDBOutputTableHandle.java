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

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TileDBOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String uri;
    private final List<TileDBColumnHandle> columnHandles;

    @JsonCreator
    public TileDBOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<TileDBColumnHandle> columnHandles,
            @JsonProperty("uri") String uri)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.uri = requireNonNull(uri, "uri is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<TileDBColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public String getURI()
    {
        return uri;
    }

    @Override
    public String toString()
    {
        return uri;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                catalogName,
                schemaName,
                tableName,
                columnHandles,
                uri);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TileDBOutputTableHandle other = (TileDBOutputTableHandle) obj;
        /*return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.columnNames, other.columnNames) &&
                Objects.equals(this.columnTypes, other.columnTypes) &&
                Objects.equals(this.uri, other.uri);*/
        return Objects.equals(this.uri, other.uri);
    }
}
