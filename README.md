# TileDB Presto Connector

[![Build Status](https://gitlab.com/TileDB-Inc/TileDB-Presto/badges/master/build.svg)](https://gitlab.com/TileDB-Inc/TileDB-Presto/pipelines)

This connector allows running sql on TileDB arrays via Presto.

TileDB is a library and format that efficiently manages large-scale,
n-dimensional, dense and sparse array data. For more information about TileDB
see the [official TileDB documentation](https://docs.tiledb.io/en/latest/introduction.html)

## Usage

The TileDB presto connector supports most sql operations from prestodb. Arrays
can be references dynamically and are not required to be "pre-registered"
with presto. No external service, such as hive is required either.
 
Predicates pushdown is supported for dimension fields.

To query a tiledb array simply reference the URI in the from

clause. i.e:

```
show columns from tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global"
```

Example select:

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
`table_name` will be the array uri and can be local or remote (s3).

For more examples see [docs/Examples.md](docs/Examples.md) .
For connector specific sql see [docs/SQL.md](docs/SQL.md)

## Docker

A quickstart docker image is available. The docker image will start a single
node presto cluster and open the cli presto interface where sql can be run.
The docker images includes two example tiledb arrays
`/opt/tiledb_example_arrays/dense_global` and
`/opt/tiledb_example_arrays/sparse_global` .

```
docker run -it --rm tiledb/presto-tiledb

presto> show columns from tiledb.tiledb."file:///opt/tiledb_example_arrays/dense_global"
```

### Mounting an Existing Local Array

It is possible to mount a local array into the docker container to allow
queries against it. You can use the `-v` option to mount a local path with
docker.

```
docker run -it --rm -v /local/array/path:/data/local_array tiledb/presto-tiledb

presto> show columns from tiledb.tiledb."file:///opt/local_array"
```

In the above example, replace `/local/array/path` with the path to the
array folder on your local machine. The `/data/local_array` path is also
arbitrary. Where ever you mount the folder inside the docker image is the
path you will use to query the array. 

## Installation

Currently this connector is built as a plugin. It must be packaged and
installed on the presto instances.

### Latest Release

Download the [latest release](https://github.com/TileDB-Inc/presto-tiledb/releases/latest)
and skip to the section
[Installation on existing Presto instance](#Installation-on-existing-Presto-instance)

### Building Connector From Source

The tiledb connector can be built using the following command from the
top level directory of the presto source.
```
./mvnw package
```

Tests can be skipped by adding `-DskipTests`

```
./mvnw package -DskipTests
```

### Installation on existing Presto instance

If you are installing the plugin on an existing presto, such as amazon
EMR, then you need to copy the `target/presto-tiledb-$VERSION` folder
to a `tiledb` directory under the plugin directory on echo presto node.

#### EMR Instruction

Using amazon emr `target/presto-tiledb-$VERSION` needs to be copied to
`/usr/lib/presto/plugin/tiledb/`

### Configuration

See [docs/Configuration.md](docs/Configuration.md)

## Limitations

See [docs/Limitations.md](docs/Limitations.md)

## Mapping of An Array to Presto Table

When a multi-dimensional is queried in presto, the dimension are converted
to column for the result set. Attributes are also returned as columns.

### Dense

Consider the following example, a dense 2 dimensional arrays with dim1 and dim2
as the dimensions and a single attribute a. The array looks like:

```
+-------+-------+
|   A   |   A   |
|   1   |   2   |
|       |       |
+---------------+
|   A   |   A   |
|   3   |   4   |
|       |       |
+---------------+
|   A   |   A   |
|   5   |   6   |
|       |       |
+---------------+
|   A   |   A   |
|   7   |   8   |
|       |       |
+-------+-------+
````

When queried via Presto the results are mapped to a table in the form of:

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

### Sparse

A sparse array is treated similar to dense. In this example the sparse array
has two dimensions, dim1 and dim2 and a single attribute A. With a sparse
array only the non-empty cells are returned.

Array:
```
+-------+-------+-------+-------+
|   A   |       |       |       |
|   1   |       |       |       |
|       |       |       |       |
+-------------------------------+
|       |       |   A   |   A   |
|       |       |   3   |   2   |
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

Presto table results:
```
 dim1 | dim2 | a
------+------+---
    1 |    1 | 1
    2 |    4 | 2
    2 |    3 | 3
```