# Limitations

The TileDB connector supports most of Presto functionality. Below is a
list of the features not currently supported.

## Encrypted Arrays

The connector does not currently support creating/writing/reading
encrypted arrays

## OpenAt Timestamp

The connector does not currently support the TileDB `openAt` functionality to
open an array at a specific timestamp. 

## Datatypes

TileDB Presto connector supports the following SQL datatypes:

-   BOOLEAN
-   TINYINT
-   INTEGER 
-   BIGINT
-   REAL
-   DOUBLE
-   DECIMAL (treated as doubles)
-   [STRING*](#variable-length-charvarchar-fields)
-   [VARCHAR*](#variable-length-charvarchar-fields)
-   [CHAR*](#variable-length-charvarchar-fields)
-   VARBINARY

No other datatypes are supported.

### Unsigned Integers 

The TileDB Presto connector does not have full support for unsigned values.
Presto and all connectors are written in Java, and Java does not have unsigned
values. As a result of this Java limitation, an unsigned 64-bit integer can
overflow if it is larger than `2^63 - 1`. Unsigned integers that are 8, 16 or
32 bits are treated as larger integers. For instance, an unsigned 32-bit value
is read into a Java type of `long`.

## Variable-length Char/Varchar fields

For `varchar`, and `char` datatypes the special case of `char(1)` or `varchar(1)`
is stored on disk as a fixed-sized attribute of size 1. Any `char`/`varchar` greater
than 1 is stored as a variable-length attribute in TileDB. TileDB **will not** enforce
the length parameter but Presto will for inserts.

## Decimal Type

Decimal types are currently treated as doubles. TileDB does not enforce the
precision or scale of the decimal types.

## Create Table

Create table is supported, however only a limited subset of TileDB parameters
is supported.

-   No support for creating encrypted arrays
-   No support for setting custom filters on attributes, coordinates or offsets


## Splits

The current split implementation is naive and splits domains evenly 
with user defined predicates (`WHERE` clause) or from the non-empty domains.
This even splitting will likely produce sub optimal splits for sparse
domains. Future work will move splitting into core TileDB where better
heuristics will be used to produce even splits.

For now, if splits are highly uneven consider increasing the number of splits
via the `tiledb.splits` session parameter or add where clauses to limit the
data set to non-empty regions of the array.