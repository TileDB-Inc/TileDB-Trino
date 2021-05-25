# Examples

Below are various examples for querying data with the TileDB Trino connector.

## SQL Examples

### Selecting Data

Typical select statements work as expected. This include predicate pushdown
for dimension fields.

Select all columns and all data from an array:
```sql
SELECT * FROM tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global"
```

Select subset of columns:
```sql
SELECT rows, cols FROM tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global"
```

Select with predicate pushdown:
```sql
SELECT * FROM tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global" WHERE rows between 1 and 2
```

### Showing Query Plans

Get the query plan without running the query:
```sql
EXPLAIN SELECT * FROM tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global" WHERE rows between 1 and 2
```

Analyze the query but running and profiling:
```sql
EXPLAIN ANALYZE SELECT * FROM tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global" WHERE rows between 1 and 2
```

## Creating a TileDB Array

It is possible to create TileDB array from Trino. Not all array schema
options are currently supported from Trino though (see [Limitations](Limitations.md#create-table)
for more details).

Minimum create table:
```sql
CREATE TABLE region(
  regionkey bigint WITH (dimension=true),
  name varchar,
  comment varchar
  ) WITH (uri = 's3://bucket/region')
```

Create table with all options specified:

```sql
CREATE TABLE region(
  regionkey bigint WITH (dimension=true, lower_bound=0L, upper_bound=3000L, extent=50L)
  name varchar,
  comment varchar
  ) WITH (uri = 's3://bucket/region', type = 'SPARSE', cell_order = 'COL_MAJOR', tile_order = 'ROW_MAJOR', capacity = 10L)
```

## Inserting Data

Data can be inserted into TileDB arrays through Trino. Inserts can be from
another table or individual values.

Copy data from one table to another:

```sql
INSERT INTO tiledb.tiledb."s3://bucket/region" select * from tpch.tiny.region
```

Data can be inserted using the `VALUES` method for single row inserts. This is
not recommended because each insert will create a new fragment and cause
degraded read performance as the number of fragments increases.

```sql
INSERT INTO tiledb.tiledb."s3://bucket/region" VALUES (1, "Test Region", "Example")
```