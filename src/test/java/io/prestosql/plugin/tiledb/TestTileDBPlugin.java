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

import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestTileDBPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new TileDBPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Path testArray = Paths.get("src", "test", "resources", "tiledb_arrays", "dense_global");
        factory.create("test", ImmutableMap.of("array-uris", testArray.toString()), new TestingConnectorContext());
    }
}
