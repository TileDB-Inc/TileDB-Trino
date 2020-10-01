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

import io.prestosql.spi.PrestoException;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.prestosql.plugin.tiledb.TileDBErrorCode.TILEDB_UNEXPECTED_ERROR;
import static io.prestosql.plugin.tiledb.TileDBQueryRunner.createTileDBQueryRunner;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.tiledb.java.api.ArrayType.TILEDB_DENSE;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestTileDBQueries
        extends AbstractTestQueryFramework
{
    private Context ctx;

    public TestTileDBQueries()
    {
        super();
        try {
            ctx = new Context();
        }
        catch (TileDBError tileDBError) {
            throw new PrestoException(TILEDB_UNEXPECTED_ERROR, tileDBError);
        }
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return createTileDBQueryRunner();
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
    public void testCreate1DVectorTinyInt()
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

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(cast(0 as tinyint), 10), (cast(3 as tinyint), 13), (cast(5 as tinyint), 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), TINYINT, INTEGER)
                .row((byte) 0, 10)
                .row((byte) 3, 13)
                .row((byte) 5, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > 2 ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), TINYINT, INTEGER)
                .row((byte) 3, 13)
                .row((byte) 5, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorSmallInt()
    {
        // Smallint
        String arrayName = "test_create_smallint";
        dropArray(arrayName);
        create1DVectorSmallIntDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "smallint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(cast(0 as smallint), 10), (cast(3 as smallint), 13), (cast(5 as smallint), 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), SMALLINT, INTEGER)
                .row((short) 0, 10)
                .row((short) 3, 13)
                .row((short) 5, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > 2 ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), SMALLINT, INTEGER)
                .row((short) 3, 13)
                .row((short) 5, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorInteger()
    {
        // Integer
        String arrayName = "test_create_integer";
        dropArray(arrayName);
        create1DVectorIntegerDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "integer", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0, 10), (3, 13), (5, 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), INTEGER, INTEGER)
                .row((int) 0, 10)
                .row((int) 3, 13)
                .row((int) 5, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > 2 ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), INTEGER, INTEGER)
                .row((int) 3, 13)
                .row((int) 5, 15)
                .build());

        MaterializedResult r = computeActual("SELECT * FROM " + arrayName);

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorString()
    {
        // Integer
        String arrayName = "test_create_string";
        dropArray(arrayName);
        create1DVectorStringDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();

        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "varchar", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "('abc', 1), ('def', 2), ('ghi', 3)", arrayName);

        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);

        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, INTEGER)
                .row("abc", 1)
                .row("def", 2)
                .row("ghi", 3)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x >= 'def' ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, INTEGER)
                .row("def", 2)
                .row("ghi", 3)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > 'abc' AND x < 'ghi'", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, INTEGER)
                .row("def", 2)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorTimestamp()
    {
        // Integer
        String arrayName = "test_create_timestamp";
        dropArray(arrayName);
        create1DVectorTimestampDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();

        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "timestamp(3)", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(timestamp '2012-10-10 10:00', 10), (timestamp '2012-11-10 10:00', 13), (timestamp '2012-12-10 10:00', 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);

        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), TIMESTAMP, INTEGER)
                .row(LocalDateTime.of(2012, 10, 10, 10, 0, 0), 10)
                .row(LocalDateTime.of(2012, 11, 10, 10, 0, 0), 13)
                .row(LocalDateTime.of(2012, 12, 10, 10, 0, 0), 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > timestamp '2012-11-10 10:00' ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), TIMESTAMP, INTEGER)
                .row(LocalDateTime.of(2012, 12, 10, 10, 0, 0), 15)
                .build());

        MaterializedResult r = computeActual("SELECT * FROM " + arrayName);

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorDate()
    {
        // Integer
        String arrayName = "test_create_date";
        dropArray(arrayName);
        create1DVectorDateDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();

        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "date", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(date '2012-10-10', 10), (date '2012-11-10', 13), (date '2012-12-10', 15)", arrayName);

        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);

        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), DATE, INTEGER)
                .row(LocalDate.of(2012, 10, 10), 10)
                .row(LocalDate.of(2012, 11, 10), 13)
                .row(LocalDate.of(2012, 12, 10), 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > date '2012-11-10' ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), DATE, INTEGER)
                .row(LocalDate.of(2012, 12, 10), 15)
                .build());

        MaterializedResult r = computeActual("SELECT * FROM " + arrayName);

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorYear() throws Exception
    {
        // Integer
        String arrayName = "test_create_year";
        dropArray(arrayName);
        createYearArray(arrayName);
        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();

        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("d1", "integer", "", "Dimension")
                        .row("ms", "timestamp(3)", "", "Attribute")
                        .row("sec", "timestamp(3)", "", "Attribute")
                        .row("min", "timestamp(3)", "", "Attribute")
                        .row("hour", "timestamp(3)", "", "Attribute")
                        .row("day", "date", "", "Attribute")
                        .row("week", "date", "", "Attribute")
                        .row("month", "date", "", "Attribute")
                        .row("year", "date", "", "Attribute")
                        .build());
        MaterializedResult r = computeActual("SELECT * FROM " + arrayName);

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorBigInt()
    {
        // BigInt
        String arrayName = "test_create_bigint";
        dropArray(arrayName);
        create1DVector(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

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

        selectSql = format("SELECT * FROM %s WHERE x > 2 ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                .row((long) 3, 13)
                .row((long) 5, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorReal()
    {
        // Real
        String arrayName = "test_create_real";
        dropArray(arrayName);
        create1DVectorRealDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "real", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0.0, 10), (3.0, 13), (5.0, 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), REAL, INTEGER)
                .row((float) 0.0, 10)
                .row((float) 3.0, 13)
                .row((float) 5.0, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > 2 ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), REAL, INTEGER)
                .row((float) 3.0, 13)
                .row((float) 5.0, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreate1DVectorDouble()
    {
        // Double
        String arrayName = "test_create_double";
        dropArray(arrayName);
        create1DVectorDoubleDimension(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "double", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0.0, 10), (3.0, 13), (5.0, 15)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), DOUBLE, INTEGER)
                .row((double) 0, 10)
                .row((double) 3, 13)
                .row((double) 5, 15)
                .build());

        selectSql = format("SELECT * FROM %s WHERE x > 2 ORDER BY x ASC", arrayName);
        selectResult = computeActual(selectSql);
        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), DOUBLE, INTEGER)
                .row((double) 3.0, 13)
                .row((double) 5.0, 15)
                .build());

        dropArray(arrayName);
    }

    @Test
    public void testCreateTableWithoutURI()
    {
        // Integer
        String arrayName = "test_create_string";
        create1DVectorStringDimensionWithoutURI(arrayName);

        File f = new File(arrayName);

        assertTrue(f.exists());

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

    @Test
    public void testInsertOrderIssue()
    {
        String arrayName = "test_insert_order_issues_base_table";
        dropArray(arrayName);
        String insertArrayName = "test_insert_order_issues_insert_table";
        dropArray(insertArrayName);

        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "y bigint WITH (dimension=true, lower_bound=0, upper_bound=10), " +
                "x bigint WITH (dimension=true, lower_bound=20, upper_bound=30), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);

        String createSql2 = format("CREATE TABLE %s(" +
                "y bigint WITH (dimension=true, lower_bound=0, upper_bound=10), " +
                "x bigint WITH (dimension=true, lower_bound=20, upper_bound=30), " +
                "a1 integer" +
                ") WITH (uri='%s')", insertArrayName, insertArrayName);
        queryRunner.execute(createSql2);

        StringBuilder builder = new StringBuilder();
        for (int y = 0; y < 1; y++) {
            for (int x = 20; x < 21; x++) {
                builder.append(format("(%s, %s, %s)", y, x, (y + x) * 100));
                if (y < 1 - 1 || x < 21 - 1) {
                    builder.append(", ");
                }
            }
        }

        String insertSql = format("INSERT INTO %s (y, x, a1) VALUES %s", arrayName, builder.toString());
        getQueryRunner().execute(insertSql);

        String insertSelectSql = format("INSERT INTO %s (y, x, a1) SELECT y, x, a1 FROM %s", insertArrayName, arrayName);
        getQueryRunner().execute(insertSelectSql);

        insertSelectSql = format("INSERT INTO %s (y, x, a1) SELECT * FROM %s", insertArrayName, arrayName);
        getQueryRunner().execute(insertSelectSql);

        insertSelectSql = format("INSERT INTO %s SELECT y, x, a1 FROM %s", insertArrayName, arrayName);
        getQueryRunner().execute(insertSelectSql);

        insertSelectSql = format("INSERT INTO %s  SELECT * FROM %s", insertArrayName, arrayName);
        getQueryRunner().execute(insertSelectSql);

        dropArray(arrayName);
        dropArray(insertArrayName);
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

    private void create1DVectorTimestampDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x timestamp WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorDateDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x date WITH (dimension=true), " +
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

    private void create1DVectorStringDimension(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x varchar WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorStringDimensionWithoutURI(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x varchar WITH (dimension=true), " +
                "a1 integer" +
                ")", arrayName);
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

    private void createYearArray(String arrayName) throws TileDBError
    {
        removeArrayIfExists(arrayName);
        // Create dimensions
        Dimension d1 =
                new Dimension(ctx, "d1", Datatype.TILEDB_INT32, new Pair(0, 3), 4);

        // Create domain
        Domain domain = new Domain(ctx);
        domain.addDimension(d1);

        // Create attribute
        Attribute ms = new Attribute(ctx, "ms", Datatype.TILEDB_DATETIME_MS);
        Attribute sec = new Attribute(ctx, "sec", Datatype.TILEDB_DATETIME_MS);
        Attribute min = new Attribute(ctx, "min", Datatype.TILEDB_DATETIME_MIN);
        Attribute hour = new Attribute(ctx, "hour", Datatype.TILEDB_DATETIME_HR);
        Attribute day = new Attribute(ctx, "day", Datatype.TILEDB_DATETIME_DAY);
        Attribute week = new Attribute(ctx, "week", Datatype.TILEDB_DATETIME_WEEK);
        Attribute month = new Attribute(ctx, "month", Datatype.TILEDB_DATETIME_MONTH);
        Attribute year = new Attribute(ctx, "year", Datatype.TILEDB_DATETIME_YEAR);

        // Create schema
        ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
        schema.setDomain(domain);
        schema.addAttribute(ms);
        schema.addAttribute(sec);
        schema.addAttribute(min);
        schema.addAttribute(hour);
        schema.addAttribute(day);
        schema.addAttribute(week);
        schema.addAttribute(month);
        schema.addAttribute(year);

        Array.create(arrayName, schema);

        ByteBuffer msBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        msBuffer.putLong(Timestamp.valueOf(LocalDate.of(2012, 10, 10).atStartOfDay()).toInstant().toEpochMilli());
        msBuffer.putLong(Timestamp.valueOf(LocalDate.of(2015, 10, 10).atStartOfDay()).toInstant().toEpochMilli());
        msBuffer.putLong(Timestamp.valueOf(LocalDate.of(2017, 10, 10).atStartOfDay()).toInstant().toEpochMilli());
        msBuffer.putLong(Timestamp.valueOf(LocalDate.of(2020, 10, 10).atStartOfDay()).toInstant().toEpochMilli());

        ByteBuffer secBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        secBuffer.putLong(60L);
        secBuffer.putLong(120L);
        secBuffer.putLong(180L);
        secBuffer.putLong(240L);

        ByteBuffer minBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        minBuffer.putLong(60L);
        minBuffer.putLong(120L);
        minBuffer.putLong(180L);
        minBuffer.putLong(240L);

        ByteBuffer hourBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        hourBuffer.putLong(60L);
        hourBuffer.putLong(120L);
        hourBuffer.putLong(180L);
        hourBuffer.putLong(240L);

        ByteBuffer dayBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        dayBuffer.putLong(60L);
        dayBuffer.putLong(120L);
        dayBuffer.putLong(180L);
        dayBuffer.putLong(240L);

        ByteBuffer weekBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        weekBuffer.putLong(60L);
        weekBuffer.putLong(120L);
        weekBuffer.putLong(180L);
        weekBuffer.putLong(240L);

        ByteBuffer monthBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        monthBuffer.putLong(60L);
        monthBuffer.putLong(120L);
        monthBuffer.putLong(180L);
        monthBuffer.putLong(240L);

        ByteBuffer yearsBuffer = ByteBuffer.allocateDirect(8 * 4).order(ByteOrder.nativeOrder());
        yearsBuffer.putLong(20L);
        yearsBuffer.putLong(30L);
        yearsBuffer.putLong(40L);
        yearsBuffer.putLong(50L);

        Array array = new Array(ctx, arrayName, TILEDB_WRITE);

        try (Query query = new Query(array, TILEDB_WRITE)) {
            query
                    .setLayout(TILEDB_ROW_MAJOR)
                    .setBuffer("ms", msBuffer)
                    .setBuffer("sec", msBuffer)
                    .setBuffer("min", secBuffer)
                    .setBuffer("hour", hourBuffer)
                    .setBuffer("day", dayBuffer)
                    .setBuffer("week", weekBuffer)
                    .setBuffer("month", monthBuffer)
                    .setBuffer("year", yearsBuffer);
            // Submit query
            query.submit();
        }
    }
}
