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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.tiledb.TileDBErrorCode.TILEDB_UNEXPECTED_ERROR;
import static com.facebook.presto.plugin.tiledb.TileDBQueryRunner.createTileDBQueryRunner;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestTileDBQueries
        extends AbstractTestQueryFramework
{
    private Context ctx;

    public TestTileDBQueries()
    {
        super(() -> createTileDBQueryRunner(ImmutableList.of(), ImmutableMap.of(), new Context()));
        try {
            ctx = new Context();
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
        }
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
    }

    @Test
    public void testCreate1DVector()
    {
        String arrayName = "test_create";
        dropArray(arrayName);
        create1DVector(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreateAllDataTypes()
    {
        String arrayName = "test_create_all_data_type";
        dropArray(arrayName);
        createAllDataTypes(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "Dimension")
                        .row("atinyint", "tinyint", "", "Attribute")
                        .row("asmallint", "smallint", "", "Attribute")
                        .row("ainteger", "integer", "", "Attribute")
                        .row("abigint", "bigint", "", "Attribute")
                        .row("areal", "real", "", "Attribute")
                        .row("adouble", "double", "", "Attribute")
                        .row("avarchar", "varchar", "", "Attribute")
                        .row("achar", "varchar(1)", "", "Attribute")
                        .row("avarchar1", "varchar(1)", "", "Attribute")
                        .row("achar1", "varchar(1)", "", "Attribute")
                        .row("avarbinary", "tinyint", "", "Attribute")
                        .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorAllDimensions()
    {
        String arrayName = "test_create_tinyint";
        // Tinyint
        dropArray(arrayName);
        create1DVectorTinyIntDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "tinyint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);

        // Smallint
        arrayName = "test_create_smallint";
        dropArray(arrayName);
        create1DVectorSmallIntDimension(arrayName);

        desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "smallint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);

        // Integer
        arrayName = "test_create_integer";
        dropArray(arrayName);
        create1DVectorIntegerDimension(arrayName);

        desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "integer", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);

        // BigInt
        arrayName = "test_create_bigint";
        dropArray(arrayName);
        create1DVector(arrayName);

        desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);

        // Real
        arrayName = "test_create_real";
        dropArray(arrayName);
        create1DVectorRealDimension(arrayName);

        desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "real", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);

        // Double
        arrayName = "test_create_double";
        dropArray(arrayName);
        create1DVectorDoubleDimension(arrayName);

        desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "double", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        dropArray(arrayName);
    }

    @Test
    public void testInsert()
    {
        String arrayName = "test_insert";
        create1DVector(arrayName);

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0, 10), (3, 13), (5, 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 0, 10)
                .row((long) 3, 13)
                .row((long) 5, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testDimensionSlice()
    {
        String arrayName = "test_dim_slice";
        create1DVector(arrayName);

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0, 10), (3, 13), (5, 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql;
        MaterializedResult selectResult;

        selectSql = format("SELECT * FROM %s WHERE (x > 0) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 3, 13)
                .row((long) 5, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x < 5) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 0, 10)
                .row((long) 3, 13)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x <= 3) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 0, 10)
                .row((long) 3, 13)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x = 3) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 3, 13)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x < 5 AND x > 0) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 3, 13)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x <= 3 AND x > 0) OR (x = 0) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 0, 10)
                .row((long) 3, 13)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (MOD(x, 2) = 1) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 3, 13)
                .row((long) 5, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x = 0 OR x = 5) ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 0, 10)
                .row((long) 5, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void test2DSlice()
    {
        String arrayName = "test_2d_slice";
        dropArray(arrayName);
        create2DArray(arrayName);

        StringBuilder builder = new StringBuilder();
        final int nrows = 100;
        final int ncols = 100;
        for (int i = 0; i < nrows; i++) {
            for (int j = 0; j < ncols; j++) {
                builder.append(format("(%s, %s, %s)", i, j, i + j));
                if (i < nrows - 1 || j < ncols - 1) {
                    builder.append(", ");
                }
            }
        }

        String insertSql = format("INSERT INTO %s (y, x, a1) VALUES %s", arrayName, builder.toString());
        getQueryRunner().execute(insertSql);

        String selectSql;
        MaterializedResult selectResult;

        selectSql = format("SELECT * FROM %s WHERE (x < 3 AND y < 2) ORDER BY y ASC, x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, BIGINT, INTEGER)
                .row((long) 0, (long) 0, 0)
                .row((long) 0, (long) 1, 1)
                .row((long) 0, (long) 2, 2)
                .row((long) 1, (long) 0, 1)
                .row((long) 1, (long) 1, 2)
                .row((long) 1, (long) 2, 3)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x BETWEEN 4 AND 5) AND (y BETWEEN 2 AND 3) ORDER BY y ASC, x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, BIGINT, INTEGER)
                .row((long) 2, (long) 4, 6)
                .row((long) 2, (long) 5, 7)
                .row((long) 3, (long) 4, 7)
                .row((long) 3, (long) 5, 8)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (x BETWEEN 4 AND 5) AND (y = 2) ORDER BY y ASC, x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, BIGINT, INTEGER)
                .row((long) 2, (long) 4, 6)
                .row((long) 2, (long) 5, 7)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (y = 2) AND ((x BETWEEN 4 AND 5) OR (X = 19)) ORDER BY y ASC, x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, BIGINT, INTEGER)
                .row((long) 2, (long) 4, 6)
                .row((long) 2, (long) 5, 7)
                .row((long) 2, (long) 19, 21)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (y = 2) AND ((x BETWEEN 4 AND 5) AND (X = 19))", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, BIGINT, INTEGER)
                .build());

        selectSql = format("SELECT * FROM %s WHERE (y = 2) AND ((x BETWEEN 4 AND 6) AND (X BETWEEN 5 AND 7)) ORDER BY y ASC, x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, BIGINT, INTEGER)
                .row((long) 2, (long) 5, 7)
                .row((long) 2, (long) 6, 8)
                .build());

        dropArray(arrayName);
    }

    private void create1DVector(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x bigint WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorTinyIntDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x tinyint WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorSmallIntDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x smallint WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorIntegerDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x integer WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorRealDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x real WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorDoubleDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x double WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create2DArray(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "y bigint WITH (dimension=true), " +
                "x bigint WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void createAllDataTypes(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x bigint WITH (dimension=true), " +
                "aTinyInt tinyint, " +
                "aSmallInt smallint, " +
                "aInteger integer, " +
                "aBigInt bigint, " +
                "aReal real, " +
                "aDouble double, " +
                "aVarchar varchar, " +
                "aChar char, " +
                "aVarchar1 varchar(1), " +
                "aChar1 char(1), " +
                "aVarBinary varbinary" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void dropArray(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String dropSql = format("DROP TABLE IF EXISTS %s", arrayName);
        queryRunner.execute(dropSql);
    }

    private void removeArrayIfExists(String arrayName)
    {
        try {
            TileDBObject.remove(ctx, arrayName);
        }
        catch (Exception e) {
            // Do nothing
        }
    }
}
