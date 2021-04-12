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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.tiledb.java.api.Context;
import io.tiledb.java.api.TileDBError;
import org.intellij.lang.annotations.Language;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static io.tiledb.java.api.Array.exists;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TileDBQueryRunner
{
    private static final Logger LOG = Logger.get(TileDBQueryRunner.class);
    private static final String TPCH_SCHEMA = "tiledb";
    private static boolean tpchLoaded;

    private TileDBQueryRunner() {}

    public static DistributedQueryRunner createTileDBQueryRunner() throws Exception
    {
        return createTileDBQueryRunner(TpchTable.getTables(), ImmutableMap.of(), ImmutableMap.of(), new Context());
    }

    public static DistributedQueryRunner createTileDBQueryRunner(ImmutableMap<String, String> sessionProperties)
            throws Exception
    {
        return createTileDBQueryRunner(TpchTable.getTables(), sessionProperties, ImmutableMap.of(), new Context());
    }

    public static DistributedQueryRunner createTileDBQueryRunner(TpchTable<?>... tables) throws Exception
    {
        return createTileDBQueryRunner(ImmutableList.copyOf(tables), ImmutableMap.of(), ImmutableMap.of(),
                new Context());
    }

    public static DistributedQueryRunner createTileDBQueryRunner(Iterable<TpchTable<?>> tables,
                                                                 ImmutableMap<String, String> sessionSystemProperties,
                                                                 Map<String, String> extraProperties, Context ctx)
            throws Exception
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tiledb")
                .setSchema("tiledb");

        for (Map.Entry<String, String> entry : sessionSystemProperties.entrySet()) {
            sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
        }

        Session session = sessionBuilder.build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(4)
                .setExtraProperties(extraProperties).build();

        try {
            List<String> existingTables = new ArrayList<>();
            List<TpchTable<?>> tablesToCopy = new ArrayList<>();
            for (TpchTable<?> table : tables) {
                if ((new File(table.getTableName())).exists()) {
                    existingTables.add(table.getTableName());
                }
                else {
                    tablesToCopy.add(table);
                }
            }

            ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder();
            if (!existingTables.isEmpty()) {
                propertiesBuilder.put("array-uris", String.join(",", existingTables));
            }

            Map<String, String> properties = propertiesBuilder.build();

            queryRunner.installPlugin(new TileDBPlugin());
            queryRunner.createCatalog("tiledb", "tiledb", properties);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            if (!tpchLoaded) {
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tablesToCopy, ctx);
                tpchLoaded = true;
            }

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables,
            Context ctx) throws TileDBError
    {
        LOG.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            // Build test tables if they do not exist. Normally we should always rebuild them but for testing
            // it is helpful to not destroy them
            if (!(new File(table.getTableName())).exists() || !exists(ctx, table.getTableName())) {
                copyTable(queryRunner, sourceCatalog, session, sourceSchema, table);
            }
        }
        LOG.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(
            QueryRunner queryRunner,
            String catalog,
            Session session,
            String schema,
            TpchTable<?> table)
    {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        @Language("SQL")
        String createSQL;
        String insertSQL;
        switch (target) {
            case "customer":
                createSQL = format("CREATE TABLE %s (\n" +
                        "    custkey bigint WITH (dimension=true),\n" +
                        "    name varchar(25),\n" +
                        "    address varchar(40),\n" +
                        "    nationkey bigint,\n" +
                        "    phone varchar(15),\n" +
                        "    acctbal double,\n" +
                        "    mktsegment varchar(10),\n" +
                        "    comment varchar(117)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (custkey, name, nationkey, address, phone, acctbal, mktsegment, comment) SELECT custkey, name, nationkey, address, phone, acctbal, mktsegment, comment FROM %s", target, source);
                break;
            case "lineitem":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    orderkey bigint WITH (dimension=true),\n" +
                        "    partkey bigint WITH (dimension=true),\n" +
                        "    suppkey bigint WITH (dimension=true),\n" +
                        "    linenumber bigint WITH (dimension=true),\n" +
                        "    quantity double,\n" +
                        "    extendedprice double,\n" +
                        "    discount double,\n" +
                        "    tax double,\n" +
                        "    returnflag varchar(1),\n" +
                        "    linestatus varchar(1),\n" +
                        "    shipdate date,\n" +
                        "    commitdate date,\n" +
                        "    receiptdate date,\n" +
                        "    shipinstruct varchar(25),\n" +
                        "    shipmode varchar(10),\n" +
                        "    comment varchar(44)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment) SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment FROM %s", target, source);
                break;
            case "nation":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    nationkey bigint WITH (dimension=true),\n" +
                        "    name varchar(25),\n" +
                        "    regionkey bigint,\n" +
                        "    comment varchar(152)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (nationkey, name, regionkey, comment) SELECT nationkey, name, regionkey, comment FROM %s", target, source);
                break;
            case "orders":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    orderkey bigint WITH (dimension=true),\n" +
                        "    custkey bigint WITH (dimension=true),\n" +
                        "    orderstatus varchar(1),\n" +
                        "    totalprice double,\n" +
                        "    orderdate date,\n" +
                        "    orderpriority varchar(15),\n" +
                        "    clerk varchar(15),\n" +
                        "    shippriority integer,\n" +
                        "    comment varchar(79)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment) SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM %s", target, source);
                break;
            case "part":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    partkey bigint WITH (dimension=true),\n" +
                        "    name varchar(55),\n" +
                        "    mfgr varchar(25),\n" +
                        "    brand varchar(10),\n" +
                        "    type varchar(25),\n" +
                        "    size integer,\n" +
                        "    container varchar(10),\n" +
                        "    retailprice double,\n" +
                        "    comment varchar(23)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (partkey, name, mfgr, brand, type, size, container, retailprice, comment) SELECT partkey, name, mfgr, brand, type, size, container, retailprice, comment FROM %s", target, source);
                break;
            case "partsupp":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    partkey bigint WITH (dimension=true),\n" +
                        "    suppkey bigint WITH (dimension=true),\n" +
                        "    availqty integer,\n" +
                        "    supplycost double,\n" +
                        "    comment varchar(199)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (partkey, suppkey, availqty, supplycost, comment) SELECT partkey, suppkey, availqty, supplycost, comment FROM %s", target, source);
                break;
            case "region":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    regionkey bigint WITH (dimension=true),\n" +
                        "    name varchar(25),\n" +
                        "    comment varchar(152)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (regionkey, name, comment) SELECT regionkey, name, comment FROM %s", target, source);
                break;
            case "supplier":
                createSQL = format("CREATE TABLE %s(\n" +
                        "    suppkey bigint WITH (dimension=true),\n" +
                        "    name varchar(25),\n" +
                        "    address varchar(40),\n" +
                        "    nationkey bigint,\n" +
                        "    phone varchar(15),\n" +
                        "    acctbal double,\n" +
                        "    comment varchar(101)\n" +
                        " ) WITH (uri = '%s')", target, target);
                insertSQL = format("INSERT INTO %s (suppkey, name, address, nationkey, phone, acctbal, comment) SELECT suppkey, name, address, nationkey, phone, acctbal, comment FROM %s", target, source);
                break;
            default:
                createSQL = format("CREATE TABLE %s LIKE %s WITH (uri = '%s')", target, source, target);
                insertSQL = format("INSERT INTO %s SELECT * FROM %s", target, source);
                break;
        }

        LOG.info("Running create table for %s", target);
        LOG.info("%s", createSQL);
        queryRunner.execute(createSQL);
        LOG.info("Running import for %s", target);
        LOG.info("%s", insertSQL);
        long start = System.nanoTime();
        long rows = queryRunner.execute(insertSQL).getUpdateCount().getAsLong();
        LOG.info("Imported %s rows for %s in %s", rows, target, nanosSince(start));
    }

    /*public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createTileDBQueryRunner(ImmutableList.copyOf(ORDERS), ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
        Logger log = Logger.get(TileDBQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }*/

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("tiledb")
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}
