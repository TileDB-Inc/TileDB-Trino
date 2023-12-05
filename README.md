# TileDB Trino Connector

![TileDB-Trino CI](https://github.com/TileDB-Inc/TileDB-Trino/actions/workflows/github_actions.yml/badge.svg)

TileDB is an efficient library for managing large-scale,
multi-dimensional dense and sparse array data introducing a novel array format. For more information about TileDB
see the [official TileDB documentation](https://docs.tiledb.io/en/latest/introduction.html)

This connector allows running SQL on TileDB arrays via Trino.  The TileDB-Trino interface supports column subselection on attributes and predicate pushdown on dimension fields, leading to superb performance for
projection and range queries.

## Docker

A quickstart Docker image is available. The docker image will start a single-node
Trino cluster and open the CLI Trino interface where SQL can be run.
The Docker image includes two example tiledb arrays
`/opt/tiledb_example_arrays/dense_global` and `/opt/tiledb_example_arrays/sparse_global`.
Simply build and run:

```
docker build -t tiledb-trino . 

docker run -it --rm tiledb-trino

```

or mount a local array into the Docker container with the `-v` option:

```
docker run -it --rm -v /local/array/path:/data/local_array tiledb-trino
```

In the above example, replace `/local/array/path` with the path to the
array folder on your local machine. The `/data/local_array` path is the
path you will use within the Docker image to access `/local/array/path`
(you can replace it with another path of your choice).


## Installation

Currently, this connector is built as a plugin. It must be packaged and
installed on the TrinoDB instances.

### Latest Release

Download the [latest release](https://github.com/TileDB-Inc/TileDB-Trino/releases/latest)
and skip to the section
[Installation on existing Trino instance](#Installation-on-existing-Trino-instance).

### Building Connector From Source

The TileDB connector can be built using the following command from the
top level directory of the Trino source.
```
./mvnw package
```

Tests can be skipped by adding `-DskipTests`

```
./mvnw package -DskipTests
```

### Installation on a Trino instance

First clone Trino
```
git clone https://github.com/trinodb/trino.git
```
Install Trino
```
./mvnw clean install -DskipTests
```

Create a TileDB directory
```
mkdir trino/core/trino-server/target/trino-server-***-SNAPSHOT/plugin/tiledb
```
Build and copy the TileDB-Trino jars to the TileDB directory
```
cp TileDB-Trino/target/*.jar trino/core/trino-server/target/trino-server-***-SNAPSHOT/plugin/tiledb
```
Create two nested directories "etc/catalog" which include the tiledb.properties file and move them to:
```
trino/core/trino-server/target/trino-server-***-SNAPSHOT/
```
Launch the Trino Server
```
trino/core/trino-server/target/trino-server-***-SNAPSHOT/bin/launcher run
```
Launch the Trino-CLI with the TileDB plugin
```
./trino/client/trino-cli/target/trino-cli-***-SNAPSHOT-executable.jar --schema tiledb --catalog tiledb
```

### Configuration

See [docs/Configuration.md](docs/Configuration.md).

## Limitations

See [docs/Limitations.md](docs/Limitations.md).

## Arrays as SQL Tables

When a multi-dimensional array is queried in Trino, the dimensions are converted
to table columns for the result set. TileDB array attributes are also returned as columns.

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

When queried via Trino the results are mapped to the following table:

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
The above array is modeled in Trino as a table of the form:

```
 dim1 | dim2 | a
------+------+---
    1 |    1 | 1
    2 |    4 | 2
    2 |    3 | 3
```
