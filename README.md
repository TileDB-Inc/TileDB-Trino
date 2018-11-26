# TileDB Presto Connector

[![Build Status](https://gitlab.com/TileDB-Inc/TileDB-Presto/badges/master/build.svg)](https://gitlab.com/TileDB-Inc/TileDB-Presto/pipelines)

This connector allows running sql on TileDB arrays via Presto.

## Usage

The TileDB presto connector supports most sql operations from prestodb. Arrays
can be references dynamically and are not required to be "pre-registered"
with presto. To query a tiledb array simply reference the URI in the from
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

A single configuration file is needed. The config file should be located in
the same cat
Sample file contents:
```
connector.name=tiledb
# Set read buffer to 10M per attribute
read-buffer-size=10485760
```

#### Plugin Configuration Parameters

The following parameters can be configured in the tiledb.properties and are
plugin wide.

| Name | Default | Data Type | Purpose |
| ---- | ------- | --------- | ------- |
| array-uris | "" | String (csv list) | List of arrays to preload metadata on |
| read-buffer-size | 10485760 | Integer | Set the max read buffer size per attribute |
| write-buffer-size | 10485760 | Integer | Set the max write buffer size per attribute |
| aws-access-key-id | "" | String | AWS_ACCESS_KEY_ID for s3 access |
| aws-secret-access-key | "" | String | AWS_SECRET_ACCESS_KEY for s3 access |


#### Session Parameters

The following session parameters can be set via `set session tiledb.X`.
Session parameters which have a default of "plugin config setting" use
the plugin configuration default for the equivalent setting.

| Name | Default | Data Type | Purpose |
| ---- | ------- | --------- | ------- |
| read_buffer_size | plugin config setting | Integer | Set the max read buffer size per attribute |
| write_buffer_size | plugin config setting  | Integer | Set the max write buffer size per attribute |
| aws_access_key_id | plugin config setting  | String | AWS_ACCESS_KEY_ID for s3 access |
| aws_secret_access_key | plugin config setting  | String | AWS_SECRET_ACCESS_KEY for s3 access |
| splits | -1 | Integer | Set the number of splits to use per query, -1 means splits will be equal to number of workers |
| split_only_predicates | false | Boolean | Split only based on predicates pushed down from where clause. For sparse array splitting evening across all domains can create skewed splits |
| enable_stats | false | Boolean | Enable collecting and dumping of connector stats to log |

## Limitations

-   Create table is limited and does not support all tiledb array schema
parameters
