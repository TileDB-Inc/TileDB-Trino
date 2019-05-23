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

import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static io.prestosql.spi.session.PropertyMetadata.longProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;

public class TileDBTableProperties
{
    public static final String URI = "uri";
    public static final String ArrayType = "type";
    public static final String CellOrder = "cell_order";
    public static final String TileOrder = "tile_order";
    public static final String Capacity = "capacity";
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public TileDBTableProperties(TypeManager typeManager, TileDBConfig config)
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        URI,
                        "URI for array to be created at",
                        null,
                        false),
                stringProperty(
                        ArrayType,
                        "Array type [DENSE, SPARSE]",
                        "SPARSE",
                        false),
                stringProperty(
                        CellOrder,
                        "Cell order for array [ROW_MAJOR, COL_MAJOR, GLOBAL_ORDER]",
                        "ROW_MAJOR",
                        false),
                stringProperty(
                        TileOrder,
                        "Tile order for array [ROW_MAJOR, COL_MAJOR, GLOBAL_ORDER]",
                        "ROW_MAJOR",
                        false),
                longProperty(
                        Capacity,
                        "Capacity of sparse array",
                        10000L,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getUri(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(URI);
    }

    public static String getArrayType(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(ArrayType);
    }

    public static String getCellOrder(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(CellOrder);
    }

    public static String getTileOrder(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(TileOrder);
    }

    public static Long getCapacity(Map<String, Object> tableProperties)
    {
        return (Long) tableProperties.get(Capacity);
    }
}
