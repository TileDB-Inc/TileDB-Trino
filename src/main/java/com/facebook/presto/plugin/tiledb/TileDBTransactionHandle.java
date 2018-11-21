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

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * TileDBTransactionHandle is a simple transaction handler by declaring a single enum type for a transaction.
 * Since tiledb has no transactions we use "instance level" transactions, which I believe just means a transaction
 * in presto is mapped to a connector instance's lifespan.
 */
public class TileDBTransactionHandle
        implements ConnectorTransactionHandle
{
    private final UUID uuid;

    public TileDBTransactionHandle()
    {
        this(UUID.randomUUID());
    }

    @JsonCreator
    public TileDBTransactionHandle(@JsonProperty("uuid") UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
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
        TileDBTransactionHandle other = (TileDBTransactionHandle) obj;
        return Objects.equals(uuid, other.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("uuid", uuid)
                .toString();
    }
}
