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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum TileDBErrorCode
        implements ErrorCodeSupplier
{
    // Thrown when an TileDB error is caught that we were not expecting,
    // such as when there is a c_api failure unexpectedly
    TILEDB_UNEXPECTED_ERROR(1, EXTERNAL),

    // Thrown when we can not create a tiledb context
    TILEDB_CONTEXT_ERROR(2, EXTERNAL),

    // Thrown when a table can not be created
    TILEDB_CREATE_TABLE_ERROR(3, EXTERNAL),

    // Thrown when a table errors in being dropped
    TILEDB_DROP_TABLE_ERROR(4, EXTERNAL),

    // Thrown when config errors on loading
    TILEDB_CONFIG_ERROR(5, EXTERNAL),

    // Thrown connector fails to load array
    TILEDB_CONNECTOR_ERROR(6, EXTERNAL),

    // Thrown when records cursor has error with query
    TILEDB_RECORD_CURSOR_ERROR(7, EXTERNAL),

    // Throw when records set fails
    TILEDB_RECORD_SET_ERROR(8, EXTERNAL),

    // Throw when insert fails
    TILEDB_PAGE_SINK_ERROR(9, EXTERNAL),

    // Throw when a split fails
    TILEDB_SPLIT_MANAGER_ERROR(10, EXTERNAL);

    private final ErrorCode errorCode;

    TileDBErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0600_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
