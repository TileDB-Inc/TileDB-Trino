# SQL

This document contains all custom SQL options defined by the TileDB Presto connector.

## Create Table

The following properties can be configured for creating a TileDB array in
Presto.

### Table properties

| Property | Description | Default Value | Possible Values | Required |
| -------- | ----------- | ------------- | --------------- | -------- |
| uri | URI for array to be created at | "" | * | Yes |
| type | Array Type | SPARSE | SPARSE, DENSE | No |
| cell_order | Cell order for array | ROW_MAJOR | ROW_MAJOR, COL_MAJOR, GLOBAL_ORDER | No |
| tile_order | Tile order for array | ROW_MAJOR | ROW_MAJOR, COL_MAJOR, GLOBAL_ORDER | No |
| capacity | Capacity of sparse array | 10000L | >0 | No |

### Column Properties

| Property | Description | Default Value | Possible Values | Required |
| -------- | ----------- | ------------- | --------------- | -------- |
| dimension | Is column a dimension | False | True, False | No |
| lower_bound | Domain Lower Bound | 0L | Any Long Value | No |
| upper_bound | Domain Upper Bound | Long.MAX_VALUE | Any Long Value | No |
| extent | Dimension Extent | 10L | Any Long Value | No |