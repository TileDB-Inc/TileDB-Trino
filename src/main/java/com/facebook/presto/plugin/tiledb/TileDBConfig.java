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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * TileDBConfig maps directly to a configuration file
 */
public class TileDBConfig
{
    private Set<URI> arrayURIs;

    private int readBufferSize = 1024 * 1024 * 10;

    private int writeBufferSize = 1024 * 1024 * 10;

    private String awsAccessKeyId;

    private String awsSecretAccessKey;

    private String tileDBConfig;

    public Set<URI> getArrayURIs()
    {
        return arrayURIs;
    }

    public int getReadBufferSize()
    {
        return readBufferSize;
    }

    public int getWriteBufferSize()
    {
        return writeBufferSize;
    }

    public String getAwsAccessKeyId()
    {
        return awsAccessKeyId;
    }

    public String getAwsSecretAccessKey()
    {
        return awsSecretAccessKey;
    }

    public String getTileDBConfig()
    {
        return tileDBConfig;
    }

    @ConfigDescription("A comma separated list of array uris")
    @Config("array-uris")
    public TileDBConfig setArrayURIs(String arrayURIs)
    {
        if (Strings.isNullOrEmpty(arrayURIs)) {
            this.arrayURIs = null;
            return this;
        }
        ImmutableSet.Builder<URI> builder = ImmutableSet.builder();
        try {
            for (String value : Splitter.on(',').trimResults().omitEmptyStrings().split(arrayURIs)) {
                builder.add(new URI(value));
            }
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
            throw new PrestoException(TileDBErrorCode.TILEDB_CONFIG_ERROR, e);
        }
        this.arrayURIs = builder.build();
        return this;
    }

    @ConfigDescription("Size in bytes to use for read buffers")
    @Config("read-buffer-size")
    public TileDBConfig setReadBufferSize(int readBufferSize)
    {
        this.readBufferSize = readBufferSize;
        return this;
    }

    @ConfigDescription("Size in bytes to use for write buffers")
    @Config("write-buffer-size")
    public TileDBConfig setWriteBufferSize(int writeBufferSize)
    {
        this.writeBufferSize = writeBufferSize;
        return this;
    }

    @ConfigDescription("AWS Access Key ID used for s3 access")
    @Config("aws-access-key-id")
    public TileDBConfig setAwsAccessKeyId(String awsAccessKeyId)
    {
        this.awsAccessKeyId = awsAccessKeyId;
        return this;
    }

    @ConfigDescription("AWS Secret Access Key used for s3 access")
    @Config("aws-secret-access-key")
    public TileDBConfig setAwsSecretAccessKey(String awsSecretAccessKey)
    {
        this.awsSecretAccessKey = awsSecretAccessKey;
        return this;
    }

    @ConfigDescription("TileDB config parameters in key1=value1,key2=value2 form")
    @Config("tiledb-config")
    public TileDBConfig setTileDBConfig(String tileDBConfig)
    {
        this.tileDBConfig = tileDBConfig;
        return this;
    }
}
