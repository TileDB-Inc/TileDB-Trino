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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import io.airlift.tpch.TpchTable;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBError;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_UNEXPECTED_ERROR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.tiledb.java.api.TileDBObject.remove;

@Test
public class TestTileDBIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private Context ctx;
    public TestTileDBIntegrationSmokeTest()
    {
        super(TileDBQueryRunner::createTileDBQueryRunner);
        try {
            ctx = new Context();
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
        }
    }

    protected boolean isDateTypeSupported()
    {
        return false;
    }

    protected boolean isParameterizedVarcharSupported()
    {
        return false;
    }

    @Test
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual("DESC orders").toTestTypes();
        assertEquals(actualColumns, getExpectedOrdersTableDescription(isDateTypeSupported(), isParameterizedVarcharSupported()));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        for (TpchTable<?> table : TpchTable.getTables()) {
            try {
                remove(ctx, table.getTableName());
            }
            catch (TileDBError tileDBError) {
                throw new PrestoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
            }
        }
    }

    private MaterializedResult getExpectedOrdersTableDescription(boolean dateSupported, boolean parametrizedVarchar)
    {
        String orderDateType;
        if (dateSupported) {
            orderDateType = "date";
        }
        else {
            orderDateType = "varchar";
        }
        if (parametrizedVarchar) {
            return MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                    .row("orderkey", "bigint", "", "Dimension")
                    .row("custkey", "bigint", "", "Dimension")
                    .row("orderstatus", "varchar(1)", "", "Attribute")
                    .row("totalprice", "double", "", "Attribute")
                    .row("orderdate", orderDateType, "", "Attribute")
                    .row("orderpriority", "varchar(15)", "", "Attribute")
                    .row("clerk", "varchar(15)", "", "Attribute")
                    .row("shippriority", "integer", "", "Attribute")
                    .row("comment", "varchar(79)", "", "Attribute")
                    .build();
        }
        else {
            return MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                    .row("orderkey", "bigint", "", "Dimension")
                    .row("custkey", "bigint", "", "Dimension")
                    .row("orderstatus", "varchar(1)", "", "Attribute")
                    .row("totalprice", "double", "", "Attribute")
                    .row("orderdate", orderDateType, "", "Attribute")
                    .row("orderpriority", "varchar", "", "Attribute")
                    .row("clerk", "varchar", "", "Attribute")
                    .row("shippriority", "integer", "", "Attribute")
                    .row("comment", "varchar", "", "Attribute")
                    .build();
        }
    }
}
