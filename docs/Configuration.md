# Configuration

A single configuration file is needed. The config file should be located in
the catalog folder (`/etc/presto/conf/catalog` on EMR) and named `tiledb.properties`.

Sample file contents:
```
connector.name=tiledb
# Set read buffer to 10M per attribute
read-buffer-size=10485760
```

## Plugin Configuration Parameters

The following parameters can be configured in the `tiledb.properties` and are
plugin-wide.

| Name | Default | Data Type | Purpose |
| ---- | ------- | --------- | ------- |
| array-uris | "" | String (csv list) | List of arrays to preload metadata on |
| read-buffer-size | 10485760 | Integer | Set the max read buffer size per attribute |
| write-buffer-size | 10485760 | Integer | Set the max write buffer size per attribute |
| aws-access-key-id | "" | String | AWS_ACCESS_KEY_ID for s3 access |
| aws-secret-access-key | "" | String | AWS_SECRET_ACCESS_KEY for s3 access |
| tiledb-config | "" | String | TileDB config parameters in key1=value1,key2=value2 form |


## Session Parameters

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
| enable_stats | false | Boolean | Enable collecting and dumping of connector stats to presto log |
| tiledb_config | "" | String | TileDB config parameters in key1=value1,key2=value2 form |
