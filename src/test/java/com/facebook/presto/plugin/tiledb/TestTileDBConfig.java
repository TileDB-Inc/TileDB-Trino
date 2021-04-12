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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestTileDBConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TileDBConfig.class)
                .setArrayURIs(null)
                .setReadBufferSize(10485760)
                .setWriteBufferSize(10485760)
                .setAwsAccessKeyId(null)
                .setAwsSecretAccessKey(null)
                .setTileDBConfig(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("array-uris", "file:///test")
                .put("read-buffer-size", "10")
                .put("write-buffer-size", "100")
                .put("aws-access-key-id", "")
                .put("aws-secret-access-key", "")
                .put("tiledb-config", "").build();

        TileDBConfig expected = null;
        expected = new TileDBConfig()
                .setArrayURIs("file:///test")
                .setReadBufferSize(10)
                .setWriteBufferSize(100)
                .setAwsAccessKeyId("")
                .setAwsSecretAccessKey("")
                .setTileDBConfig("");
        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitPropertyMappingsWithMultipleArrays()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("array-uris", "file:///test,s3:///test-bucket/array")
                .put("read-buffer-size", "1048576")
                .put("write-buffer-size", "1048576")
                .put("aws-access-key-id", "123")
                .put("aws-secret-access-key", "abc")
                .put("tiledb-config", "key1=value1,key2=value2").build();

        TileDBConfig expected = null;
        expected = new TileDBConfig()
                .setArrayURIs("file:///test,s3:///test-bucket/array")
                .setReadBufferSize(1048576)
                .setWriteBufferSize(1048576)
                .setAwsAccessKeyId("123")
                .setAwsSecretAccessKey("abc")
                .setTileDBConfig("key1=value1,key2=value2");
        assertFullMapping(properties, expected);
    }
}
