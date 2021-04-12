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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.tiledb.java.api.Datatype;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * TileDB Column represents a mapping of tiledb attributes/dimensions to a column type
 * We use a custom class so we can track what is a dimension vs an attribute
 *
 * The json properties are important as this class will be serialized and sent via json to the workers
 * Every property needs a getter in the form of getVariable.
 * i.e. variable = isDimension, getter = getIsDimension()
 */
public final class TileDBColumn
{
    private final String name;
    private final Type type;
    private final Datatype tileDBType;
    private final Boolean isVariableLength;
    private final Boolean isDimension;
    private final Boolean isNullable;

    @JsonCreator
    public TileDBColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("TileDBType") Datatype tileDBType,
            @JsonProperty("isVariableLength") Boolean isVariableLength,
            @JsonProperty("isDimension") Boolean isDimension,
            @JsonProperty("isNullable") Boolean isNullable)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.tileDBType = requireNonNull(tileDBType, "TileDBType is null");
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
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Boolean getNullable()
    {
        return isNullable;
    }

    @JsonProperty
    public Datatype getTileDBType()
    {
        return tileDBType;
    }

    @JsonProperty
    public Boolean getIsVariableLength()
    {
        return isVariableLength;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Boolean getIsDimension()
    {
        return isDimension;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, isNullable, isDimension);
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

        TileDBColumn other = (TileDBColumn) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.isNullable, other.isNullable) &&
                Objects.equals(this.isDimension, other.isDimension);
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }
}
