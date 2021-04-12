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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.tiledb.java.api.Datatype;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Implements a column handler
 * The json properties are important as this class will be serialized and sent via json to the workers
 * Every property needs a getter in the form of getVariable.
 * i.e. variable = isDimension, getter = getIsDimension()
 */
public final class TileDBColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final Datatype columnTileDBType;
    private final boolean isVariableLength;
    private final boolean isDimension;
    private final boolean isNullable;

    @JsonCreator
    public TileDBColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("columnTileDBType") Datatype columnTileDBType,
            @JsonProperty("isVariableLength") Boolean isVariableLength,
            @JsonProperty("isDimension") Boolean isDimension,
            @JsonProperty("isNullable") Boolean isNullable)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnTileDBType = requireNonNull(columnTileDBType, "columnTileDBType is null");
        this.isVariableLength = requireNonNull(isVariableLength, "isVariableLength is null");
        this.isDimension = requireNonNull(isDimension, "isDimension is null");
        if (isNullable == null) {
            this.isNullable = false;
        }
        else {
            this.isNullable = isNullable;
        }
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Datatype getColumnTileDBType()
    {
        return columnTileDBType;
    }

    @JsonProperty
    public Boolean getIsVariableLength()
    {
        return isVariableLength;
    }

    @JsonProperty
    public Boolean getIsDimension()
    {
        return isDimension;
    }

    @JsonProperty
    public Boolean getNullable()
    {
        return isNullable;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName, columnType, isNullable, isDimension);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        TileDBColumnHandle other = (TileDBColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.isNullable, other.isNullable) &&
                Objects.equals(this.isDimension, other.isDimension);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("columnTileDBType", columnTileDBType)
                .add("isVariableLength", isVariableLength)
                .add("isNullable", isNullable)
                .add("isDimension", isDimension)
                .toString();
    }
}
