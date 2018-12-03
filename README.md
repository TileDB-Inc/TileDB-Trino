# TileDB Presto Connector

[![Build Status](https://gitlab.com/TileDB-Inc/TileDB-Presto/badges/master/build.svg)](https://gitlab.com/TileDB-Inc/TileDB-Presto/pipelines)

TileDB is an efficient library for managing large-scale,
multi-dimensional dense and sparse array data introducing a novel array format. For more information about TileDB
see the [official TileDB documentation](https://docs.tiledb.io/en/latest/introduction.html)

This connector allows running SQL on TileDB arrays via Presto.  The TileDB-Presto interface supports column subselection on attributes and predicate pushdown on dimension fields, leading to superb performance for
projection and range queries.


## Quickstart

## Docker

A quickstart Docker image is available. The docker image will start a single-node 
Presto cluster and open the CLI Presto interface where SQL can be run.
The Docker image includes two example tiledb arrays
`/opt/tiledb_example_arrays/dense_global` and `/opt/tiledb_example_arrays/sparse_global`. 
Simply run:

```
docker run -it --rm tiledb/tiledb-presto
```

or mount a local array into the Docker container with the `-v` option: 

```
docker run -it --rm -v /local/array/path:/data/local_array tiledb/tiledb-presto
```

In the above example, replace `/local/array/path` with the path to the
array folder on your local machine. The `/data/local_array` path is the 
path you will use within the Docker image to access `/local/array/path`
(you can replace it with another path of your choice). 

The TileDB presto connector supports most SQL operations from PrestoDB. Arrays
can be referenced dynamically and are not required to be "pre-registered"
with Presto. *No external service* (such as [Apache Hive](https://hive.apache.org/)) 
is required.
 
Examples: 

```
show columns from tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global;"
Column |  Type   | Extra |  Comment  
--------+---------+-------+-----------
 rows   | integer |       | Dimension 
 cols   | integer |       | Dimension 
 a      | integer |       | Attribute 

```


```
select * from tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global" WHERE rows = 3 AND cols between 1 and 2;
 rows | cols | a 
------+------+---
    3 |    1 | 5 
    3 |    2 | 6 

```

Presto uses the form of `catalog`.`schema`.`table_name` for querying. TileDB
does not have a concept of a schema, so any valid string can be used for the 
schema name when querying. `tiledb` is used for convenience in the examples.
`table_name` is the array URI and can be local (file://) or remote (s3://).

For more examples see [docs/Examples.md](docs/Examples.md).

For custom connector SQL options see [docs/SQL.md](docs/SQL.md).

## Installation

Currently this connector is built as a plugin. It must be packaged and
installed on the PrestoDB instances.

### Latest Release

Download the [latest release](https://github.com/TileDB-Inc/presto-tiledb/releases/latest)
and skip to the section
[Installation on existing Presto instance](#Installation-on-existing-Presto-instance).

### Building Connector From Source

The TileDB connector can be built using the following command from the
top level directory of the Presto source.
```
./mvnw package
```

Tests can be skipped by adding `-DskipTests`

```
./mvnw package -DskipTests
```

### Installation on an existing Presto instance

If you are installing the plugin on an existing Presto instance, such as Amazon
EMR, you need to copy the `target/presto-tiledb-$VERSION` folder
to a `tiledb` directory under the plugin directory on echo Presto node.

#### AWS EMR 

Using Amazon EMR `target/presto-tiledb-$VERSION` needs to be copied to
`/usr/lib/presto/plugin/tiledb/`

### Configuration

See [docs/Configuration.md](docs/Configuration.md).

## Limitations

See [docs/Limitations.md](docs/Limitations.md).

## Arrays as SQL Tables

When a multi-dimensional array is queried in Presto, the dimensions are converted
to table columns for the result set. TileDB array attributes attributes are also returned as columns.

### Dense Arrays

Consider the following example 2D `4x2` dense array with `dim1` and `dim2`
as the dimensions and a single attribute `a`:

```
+-------+-------+
|       |       |
|  a:1  |  a:2  |
|       |       |
+---------------+
|       |       |
|  a:3  |  a:4  |
|       |       |
+---------------+
|       |       |
|  a:5  |  a:6  |
|       |       |
+---------------+
|       |       |
|  a:7  |  a:8  |
|       |       |
+-------+-------+
````

When queried via Presto the results are mapped to the following table:

```
 dim1 | dim2 | a
------+------+---
    1 |    1 | 1
    1 |    2 | 2
    2 |    1 | 3
    2 |    2 | 4
    3 |    1 | 5
    3 |    2 | 6
    4 |    1 | 7
    4 |    2 | 8
```

### Sparse Arrays

A sparse array is materialized similarly to dense arrays. The following example
depicts a 2D `4x4` sparse array with dimensions `dim1`, `dim2` and
a single attribute `a`. Notice that this array has mostly empty cells. 

```
+-------+-------+-------+-------+
|       |       |       |       |
|  a:1  |       |       |       |
|       |       |       |       |
+-------------------------------+
|       |       |       |       |
|       |       |  a:3  |  a:2  |
|       |       |       |       |
+-------------------------------+
|       |       |       |       |
|       |       |       |       |
|       |       |       |       |
+-------------------------------+
|       |       |       |       |
|       |       |       |       |
|       |       |       |       |
+-------+-------+-------+-------+
```

For sparse arrays only non-empty cells are materialized and returned.
The above array is modeled in Presto as a table of the form:

```
 dim1 | dim2 | a
------+------+---
    1 |    1 | 1
    2 |    4 | 2
    2 |    3 | 3
```
