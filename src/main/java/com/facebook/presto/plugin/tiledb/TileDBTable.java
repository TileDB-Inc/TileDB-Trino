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
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.EncryptionType;
import io.tiledb.java.api.TileDBError;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.plugin.tiledb.TileDBModule.prestoTypeFromTileDBType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TileDBTable
{
    private final String schema;
    private final String name;
    private final List<TileDBColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final URI uri;

    @JsonCreator
    public TileDBTable(
            @JsonProperty("name") String schema,
            @JsonProperty("name") String name,
            @JsonProperty("uri") URI uri,
            Context ctx,
            EncryptionType encryptionType,
            byte[] encryptionKey) throws TileDBError
    {
        checkArgument(!isNullOrEmpty(schema), "schema is null or is empty");
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.schema = requireNonNull(schema, "schema is null");
        this.name = requireNonNull(name, "name is null");
        this.uri = requireNonNull(uri, "uri is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        ImmutableList.Builder<TileDBColumn> columns = ImmutableList.builder();
        ArraySchema s;
        if (encryptionType != null && encryptionKey != null) {
            s = new ArraySchema(ctx, uri.toString(), encryptionType, encryptionKey);
        }
        else {
            s = new ArraySchema(ctx, uri.toString());
        }
        Domain domain = s.getDomain();
        // Add dimensions as a column
        for (Dimension dimension : domain.getDimensions()) {
            Type type = prestoTypeFromTileDBType(dimension.getType());
            columnsMetadata.add(new ColumnMetadata(dimension.getName(), type, "Dimension", null, false));
            columns.add(new TileDBColumn(dimension.getName(), type, dimension.getType(), dimension.isVar(), true, false));
            dimension.close();
        }
        // Add attribute as a column
        for (long i = 0; i < s.getAttributeNum(); i++) {
            Attribute attribute = s.getAttribute(i);
            Type type = prestoTypeFromTileDBType(attribute.getType());
            if (attribute.getType() == Datatype.TILEDB_CHAR && !attribute.isVar()) {
                type = VarcharType.createVarcharType(toIntExact(attribute.getCellValNum()));
            }
            columnsMetadata.add(new ColumnMetadata(attribute.getName(), type, "Attribute", null, false));
            columns.add(new TileDBColumn(attribute.getName(), type, attribute.getType(), attribute.isVar(), false, attribute.getNullable()));
            attribute.close();
        }
        domain.close();
        domain.close();
        s.close();
        this.columnsMetadata = columnsMetadata.build();
        this.columns = columns.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<TileDBColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public URI getURI()
    {
        return uri;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
