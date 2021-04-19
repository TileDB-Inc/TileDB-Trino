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

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.PrestoException;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import io.tiledb.java.api.Array;
import io.tiledb.java.api.ArraySchema;
import io.tiledb.java.api.Attribute;
import io.tiledb.java.api.ByteShuffleFilter;
import io.tiledb.java.api.Bzip2Filter;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.Datatype;
import io.tiledb.java.api.Dimension;
import io.tiledb.java.api.Domain;
import io.tiledb.java.api.FilterList;
import io.tiledb.java.api.GzipFilter;
import io.tiledb.java.api.NativeArray;
import io.tiledb.java.api.Pair;
import io.tiledb.java.api.Query;
import io.tiledb.java.api.TileDBError;
import io.tiledb.java.api.TileDBObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
import static io.tiledb.java.api.ArrayType.TILEDB_SPARSE;
import static io.tiledb.java.api.Layout.TILEDB_GLOBAL_ORDER;
import static io.tiledb.java.api.Layout.TILEDB_ROW_MAJOR;
import static io.tiledb.java.api.QueryType.TILEDB_WRITE;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestTileDBQueries
        extends AbstractTestQueryFramework
{
    private Context ctx;
    private String denseURI;
    private String sparseURI;

    @BeforeClass
    public void setup() throws Exception
    {
        denseURI = "dense_array";
        sparseURI = "sparse_array";
        if (Files.exists(Paths.get(denseURI))) {
            TileDBObject.remove(ctx, denseURI);
        }
        denseArrayNullableCreate();
        denseArrayNullableWrite();
        if (Files.exists(Paths.get(sparseURI))) {
            TileDBObject.remove(ctx, sparseURI);
        }
        sparseArrayNullableCreate();
        sparseArrayNullableWrite();
    }

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
    public final void destroy() throws TileDBError
    {
        if (Files.exists(Paths.get(denseURI))) {
            TileDBObject.remove(ctx, denseURI);
        }
        if (Files.exists(Paths.get(sparseURI))) {
            TileDBObject.remove(ctx, sparseURI);
        }
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
    public void testCreateHeterogeneousDimensions()
    {
        // Integer
        String arrayName = "test_create_heterogeneous";
        dropArray(arrayName);
        createHeterogeneousDimensions(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();

        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "varchar", "", "Dimension")
                        .row("y", "integer", "", "Dimension")
                        .row("z", "double", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, y, z, a1) VALUES " +
                "('abc', 1, 1.0, 1), ('def', 2, 3.0, 2), ('ghi', 3, 4.0, 3), ('ppp', 4, 1.0, 1)", arrayName);

        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s ORDER BY y ASC", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);

        assertEquals(selectResult, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, INTEGER, DOUBLE, INTEGER)
                .row("abc", 1, 1.0, 1)
                .row("def", 2, 3.0, 2)
                .row("ghi", 3, 4.0, 3)
                .row("ppp", 4, 1.0, 1)
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
    public void testCreateTableWithFilters() throws TileDBError
    {
        // Integer
        String arrayName = "test_create_with_filters";
        create1DVectorStringWithFilters(arrayName);
        Array array = new Array(new Context(), arrayName);

        FilterList d1FilterList = array.getSchema().getDomain().getDimension(0).getFilterList();
        FilterList a1FilterList = array.getSchema().getAttribute(0).getFilterList();
        FilterList offsetsFilterList = array.getSchema().getOffsetsFilterList();

        assertEquals(d1FilterList.getNumFilters(), 2);
        assertEquals(a1FilterList.getNumFilters(), 0);
        assertEquals(offsetsFilterList.getNumFilters(), 2);

        assertEquals(d1FilterList.getFilter(0).getClass(), ByteShuffleFilter.class);
        assertEquals(d1FilterList.getFilter(1).getClass(), GzipFilter.class);
        assertEquals(offsetsFilterList.getFilter(0).getClass(), Bzip2Filter.class);
        assertEquals(offsetsFilterList.getFilter(1).getClass(), GzipFilter.class);

        dropArray(arrayName);
    }

    @Test
    public void testCreateTableEncrypted() throws Exception
    {
        // Integer
        String arrayName = "test_create_encrypted";
        String encryptionKey = "0123456789abcdeF0123456789abcdeF";

        create1DVectorStringEncrypted(arrayName, encryptionKey);

        // Try with a wrong key first, we expect this to fail
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.encryption_key", "0123456789xxxxF0123456789abcdeF")
                        .build())) {
            runner.execute("SELECT * FROM " + arrayName);
            fail();
        }
        catch (Exception e) {
        }

        try (QueryRunner runner = createTileDBQueryRunner(ImmutableMap.<String, String>builder()
                .put("tiledb.encryption_key", encryptionKey)
                .build())) {
            runner.execute("SELECT * FROM " + arrayName);

            String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                    "('abc', 1), ('def', 2), ('ghi', 3)", arrayName);

            runner.execute(insertSql);

            String selectSql = format("SELECT * FROM %s ORDER BY x ASC", arrayName);
            MaterializedResult selectResult = runner.execute(selectSql);

            assertEquals(selectResult, MaterializedResult.resultBuilder(runner.getDefaultSession(), VARCHAR, INTEGER)
                    .row("abc", 1)
                    .row("def", 2)
                    .row("ghi", 3)
                    .build());
        }

        dropArray(arrayName);
    }

    @Test
    public void testTimeTraveling() throws Exception
    {
        // BigInt
        String arrayName = "test_create_bigint";
        dropArray(arrayName);
        create1DVector(arrayName);

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0, 10), (1, 13)", arrayName);

        getQueryRunner().execute(insertSql);

        insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(2, 2), (3, 13)", arrayName);

        getQueryRunner().execute(insertSql);

        insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(4, 2100), (5, 3600)", arrayName);

        getQueryRunner().execute(insertSql);

        // Retrieve all fragments and sort them in ascending order
        String[] dirs = new File(arrayName).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name)
            {
                return new File(dir, name).isDirectory() && !name.equals("__meta");
            }
        });
        Arrays.sort(dirs);

        /* Open each fragment and validate its data */

        // Check the first fragment
        String ts1 = dirs[0].split("\\_")[2];
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.timestamp", ts1)
                        .build())) {
            MaterializedResult r = runner.execute("SELECT * FROM " + arrayName + " ORDER BY x");

            assertEquals(r, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                    .row(0L, 10)
                    .row(1L, 13)
                    .build());
        }
        catch (Exception e) {
        }

        // Check the second fragment
        String ts2 = dirs[1].split("\\_")[2];
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.timestamp", ts2)
                        .build())) {
            MaterializedResult r = runner.execute("SELECT * FROM " + arrayName + " ORDER BY x");

            assertEquals(r, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                    .row(0L, 10)
                    .row(1L, 13)
                    .row(2L, 2)
                    .row(3L, 13)
                    .build());
        }
        catch (Exception e) {
        }

        // Check the third fragment
        String ts3 = dirs[2].split("\\_")[2];
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.timestamp", ts3)
                        .build())) {
            MaterializedResult r = runner.execute("SELECT * FROM " + arrayName + " ORDER BY x");

            assertEquals(r, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                    .row(0L, 10)
                    .row(1L, 13)
                    .row(2L, 2)
                    .row(3L, 13)
                    .row(4L, 2100)
                    .row(5L, 3600)
                    .build());
        }
        catch (Exception e) {
        }

        dropArray(arrayName);
    }

    @Test
    public void testTimeTravelingEncrypted() throws Exception
    {
        // BigInt
        String arrayName = "test_create_bigint";
        String encryptionKey = "0123456789abcdeF0123456789abcdeF";
        create1DVectorEncrypted(arrayName, encryptionKey);

        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.encryption_key", encryptionKey)
                        .build())) {
            String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                    "(0, 10), (1, 13)", arrayName);

            runner.execute(insertSql);

            insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                    "(2, 2), (3, 13)", arrayName);

            runner.execute(insertSql);

            insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                    "(4, 2100), (5, 3600)", arrayName);

            runner.execute(insertSql);
        }
        catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }

        // Retrieve all fragments and sort them in ascending order
        String[] dirs = new File(arrayName).list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name)
            {
                return new File(dir, name).isDirectory() && !name.equals("__meta");
            }
        });
        Arrays.sort(dirs);

        /* Open each fragment and validate its data */

        // Check the first fragment
        String ts1 = dirs[0].split("\\_")[2];
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.timestamp", ts1)
                        .put("tiledb.encryption_key", encryptionKey)
                        .build())) {
            MaterializedResult r = runner.execute("SELECT * FROM " + arrayName + " ORDER BY x");

            assertEquals(r, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                    .row(0L, 10)
                    .row(1L, 13)
                    .build());
        }
        catch (Exception e) {
            fail(e.getMessage());
        }

        // Check the second fragment
        String ts2 = dirs[1].split("\\_")[2];
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.timestamp", ts2)
                        .put("tiledb.encryption_key", encryptionKey)
                        .build())) {
            MaterializedResult r = runner.execute("SELECT * FROM " + arrayName + " ORDER BY x");

            assertEquals(r, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                    .row(0L, 10)
                    .row(1L, 13)
                    .row(2L, 2)
                    .row(3L, 13)
                    .build());
        }
        catch (Exception e) {
            fail(e.getMessage());
        }

        // Check the third fragment
        String ts3 = dirs[2].split("\\_")[2];
        try (QueryRunner runner = createTileDBQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("tiledb.timestamp", ts3)
                        .put("tiledb.encryption_key", encryptionKey)
                        .build())) {
            MaterializedResult r = runner.execute("SELECT * FROM " + arrayName + " ORDER BY x");

            assertEquals(r, MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), BIGINT, INTEGER)
                    .row(0L, 10)
                    .row(1L, 13)
                    .row(2L, 2)
                    .row(3L, 13)
                    .row(4L, 2100)
                    .row(5L, 3600)
                    .build());
        }
        catch (Exception e) {
            fail(e.getMessage());
        }

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
    /*
    =======================================================
    Nullable attributes begin
    ======================================================
     */

    /**
     * Dense array with nullable and fixed size attributes.
     *
     * @throws Exception
     */
    private void denseArrayNullableCreate() throws Exception
    {
        Dimension<Integer> rows =
                new Dimension<>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 2), 2);
        Dimension<Integer> cols =
                new Dimension<>(ctx, "cols", Integer.class, new Pair<Integer, Integer>(1, 2), 2);

        // Create and set getDomain
        Domain domain = new Domain(ctx);
        domain.addDimension(rows);
        domain.addDimension(cols);

        Attribute a1 = new Attribute(ctx, "a1", Float.class);
        Attribute a2 = new Attribute(ctx, "a2", Integer.class);
        a2.setCellValNum(1);

        a1.setNullable(true);
        a2.setNullable(true);

        ArraySchema schema = new ArraySchema(ctx, TILEDB_DENSE);
        schema.setTileOrder(TILEDB_ROW_MAJOR);
        schema.setCellOrder(TILEDB_ROW_MAJOR);
        schema.setDomain(domain);
        schema.addAttribute(a1);
        schema.addAttribute(a2);

        Array.create(denseURI, schema);
    }

    /**
     * Populating the dense array with nullable attributes.
     * @throws Exception
     */
    private void denseArrayNullableWrite() throws Exception
    {
        // Prepare cell buffers
        NativeArray a1 = new NativeArray(ctx, new float[] {2.0f, 3.0f, 4.0f, 1.0f}, Float.class);
        NativeArray a2 = new NativeArray(ctx, new int[] {1, 4, 2, 2}, Integer.class);

        // Create query
        try (Array array = new Array(ctx, denseURI, TILEDB_WRITE); Query query = new Query(array)) {
            query.setLayout(TILEDB_ROW_MAJOR);
            NativeArray a1Bytemap = new NativeArray(ctx, new short[] {0, 1, 1, 0}, Datatype.TILEDB_UINT8);
            NativeArray a2Bytemap = new NativeArray(ctx, new short[] {1, 1, 0, 1}, Datatype.TILEDB_UINT8);

            query.setBufferNullable("a1", a1, a1Bytemap);
            query.setBufferNullable("a2", a2, a2Bytemap);

            // Submit query
            query.submit();
        }
    }

    /**
     * Reads a two-dimensional dense array with nullable attributes.
     */
    @Test
    public void testRead2DVectorNullableDense()
    {
        String selectSql = format("SELECT * FROM %s", denseURI);
        MaterializedResult selectResult = computeActual(selectSql);
        List<MaterializedRow> resultRows = selectResult.getMaterializedRows();
        MaterializedResult expected = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), DOUBLE, INTEGER)
                .row(1, 2, 3.0, 4)
                .row(1, 1, null, 1)
                .row(2, 1, 4.0, null)
                .row(2, 2, null, 2)
                .build();
        //using string representation because of null values
        List<MaterializedRow> expectedRows = expected.getMaterializedRows();
        List<String> resultRowsToString = resultRows.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        List<String> expectedRowsToString = expectedRows.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        assertTrue(expectedRowsToString.size() == resultRowsToString.size() && expectedRowsToString.containsAll(resultRowsToString) && resultRowsToString.containsAll(expectedRowsToString)); //presto returns rows in different every time.
    }

    /**
     * Sparse array with nullable and variable sized attributes.
     *
     * @throws TileDBError
     */
    private void sparseArrayNullableCreate() throws TileDBError
    {
        Dimension<Integer> d1 =
                new Dimension<>(ctx, "d1", Integer.class, new Pair<Integer, Integer>(1, 8), 2);

        // Create and set getDomain
        Domain domain = new Domain(ctx);
        domain.addDimension(d1);

        Attribute a1 = new Attribute(ctx, "a1", Integer.class);
        Attribute a2 = new Attribute(ctx, "a2", Datatype.TILEDB_STRING_ASCII);
        a2.setCellVar();

        a1.setNullable(true);
        a2.setNullable(true);

        ArraySchema schema = new ArraySchema(ctx, TILEDB_SPARSE);
        schema.setTileOrder(TILEDB_ROW_MAJOR);
        schema.setCellOrder(TILEDB_ROW_MAJOR);
        schema.setDomain(domain);
        schema.addAttribute(a1);
        schema.addAttribute(a2);

        Array.create(sparseURI, schema);
    }

    /**
     * Populating the sparse array with nullable attributes.
     * @throws TileDBError
     */
    private void sparseArrayNullableWrite() throws TileDBError
    {
        NativeArray data = new NativeArray(ctx, new int[] {1, 2, 3, 4, 5}, Integer.class);

        // Prepare cell buffers
        NativeArray a1 = new NativeArray(ctx, new int[] {1, 2, 3, 4, 5}, Integer.class);

        NativeArray a2Data = new NativeArray(ctx, "aabbccddee", Datatype.TILEDB_STRING_ASCII);
        NativeArray a2Off = new NativeArray(ctx, new long[] {0, 2, 4, 6, 8}, Datatype.TILEDB_UINT64);

        // Create query
        Array array = new Array(ctx, sparseURI, TILEDB_WRITE);
        Query query = new Query(array);
        query.setLayout(TILEDB_GLOBAL_ORDER);

        NativeArray a1ByteMap =
                new NativeArray(ctx, new short[] {0, 0, 0, 1, 1}, Datatype.TILEDB_UINT8);
        NativeArray a2ByteMap =
                new NativeArray(ctx, new short[] {1, 1, 1, 0, 0}, Datatype.TILEDB_UINT8);

        query.setBuffer("d1", data);
        query.setBufferNullable("a1", a1, a1ByteMap);
        query.setBufferNullable("a2", a2Off, a2Data, a2ByteMap);

        // Submit query
        query.submit();

        query.finalizeQuery();
        query.close();
        array.close();
    }

    /**
     * Reads a two-dimensional sparse array with nullable attributes.
     */
    @Test
    public void testRead2DVectorNullableSparse()
    {
        String selectSql = format("SELECT * FROM %s", sparseURI);
        MaterializedResult selectResult = computeActual(selectSql);
        List<MaterializedRow> resultRows = selectResult.getMaterializedRows();
//        for (MaterializedRow row : resultRows) {
//            System.out.println(row);
//        }
        MaterializedResult expected = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), INTEGER, VARCHAR)
                .row(1, null, "aa")
                .row(2, null, "bb")
                .row(3, null, "cc")
                .row(4, 4, null)
                .row(5, 5, null)
                .build();
        //using string representation because of null values
        List<MaterializedRow> expectedRows = expected.getMaterializedRows();
        List<String> resultRowsToString = resultRows.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        List<String> expectedRowsToString = expectedRows.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        assertTrue(expectedRowsToString.size() == resultRowsToString.size() && expectedRowsToString.containsAll(resultRowsToString) && resultRowsToString.containsAll(expectedRowsToString)); //presto returns rows in different every time.
    }

    /**
     * Writes an one-dimensional array with nullable attributes.
     */
    @Test
    public void testWrite1DVectorNullableSparse()
    {
        String arrayName = "test_create_nullable";
        dropArray(arrayName);
        create1DVectorNullableSparse(arrayName);

        MaterializedResult desc = computeActual(format("DESC %s", arrayName)).toTestTypes();
        assertEquals(desc,
                MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "Dimension")
                        .row("a1", "integer", "", "Attribute")
                        .build());

        String insertSql = format("INSERT INTO %s (x, a1) VALUES " +
                "(0, 3), (3, null), (5, null)", arrayName);
        getQueryRunner().execute(insertSql);

        String selectSql = format("SELECT * FROM %s", arrayName);
        MaterializedResult selectResult = computeActual(selectSql);
        List<MaterializedRow> resultRows = selectResult.getMaterializedRows();
//        for (MaterializedRow row : resultRows){
//            System.out.println(row);
//        }
        MaterializedResult expected = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), INTEGER)
                .row(0, 3)
                .row(3, null)
                .row(5, null)
                .build();
        //using string representation because of null values
        List<MaterializedRow> expectedRows = expected.getMaterializedRows();
        List<String> resultRowsToString = resultRows.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        List<String> expectedRowsToString = expectedRows.stream()
                .map(object -> Objects.toString(object, null))
                .collect(Collectors.toList());
        assertTrue(expectedRowsToString.size() == resultRowsToString.size() && expectedRowsToString.containsAll(resultRowsToString) && resultRowsToString.containsAll(expectedRowsToString)); //presto returns rows in different every time.
        dropArray(arrayName);
    }
    /*
    =======================================================
    Nullable attributes end
    ======================================================
     */

    private void create1DVector(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x bigint WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorNullableSparse(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x bigint WITH (dimension=true), " +
                "a1 integer WITH (nullable=true)" +
                ") WITH (uri='%s')", arrayName, arrayName); //SPARSE is the default value
        queryRunner.execute(createSql);
    }

    private void create1DVectorNullableDense(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x bigint WITH (dimension=true), " +
                "a1 integer WITH (nullable=true)" +
                ") WITH (uri='%s', type='%s')", arrayName, arrayName, "DENSE");
        queryRunner.execute(createSql);
    }

    private void create1DVectorEncrypted(String arrayName, String encryptionKey)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x bigint WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s', encryption_key='%s')", arrayName, arrayName, encryptionKey);
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

    private void createHeterogeneousDimensions(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x varchar WITH (dimension=true), " +
                "y integer WITH (dimension=true), " +
                "z double WITH (dimension=true), " +
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

    private void create1DVectorStringWithFilters(String arrayName)
    {
        QueryRunner queryRunner = getQueryRunner();
        String createSql = format("CREATE TABLE %s(" +
                "x varchar WITH (dimension=true,filter_list='(byteshuffle), (gzip,9)'), " +
                "a1 integer" +
                ") WITH (uri='%s',offsets_filter_list='(bzip2,-1), (gzip,3)')", arrayName, arrayName);
        queryRunner.execute(createSql);
    }

    private void create1DVectorStringEncrypted(String arrayName, String encryptionKey)
    {
        QueryRunner queryRunner = getQueryRunner();

        String createSql = format("CREATE TABLE %s(" +
                "x varchar WITH (dimension=true), " +
                "a1 integer" +
                ") WITH (uri='%s',encryption_key='%s')", arrayName, arrayName, encryptionKey);

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
