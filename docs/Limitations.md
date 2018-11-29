# Limitations

The TileDB connect support most of presto functionality, below is a
list of the feature not currently supported.

## Encrypted Arrays

The connector does not currently support reading or creating
encrypted arrays

## OpenAt Timestamp

The connector does not currently support the TileDB openAt functionality to
open an array at a specific timestamp. 

## Datatypes

TileDB supports the following sql datatypes:

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
values. As a result of this Java limitation, unsigned 64bit integers can
overflow if it is larger than `2^63 - 1`. Unsigned integers that are 8, 16 or
32 bits are treated as larger integers. For instance a unsigned 32bit is read
into a Java type of `long`.

## Variable Length Char/Varchar fields

For varchar, and char datatypes the special case of char(1) or varchar(1)
are stored on disk as fixed size attributes of size 1. Any char/varchar greater
than 1 is stored as a variable length in TileDB. TileDB **will not** enforce
the length parameter but presto does for inserts.

## Decimal Type

Decimal types are currently treated as doubles. TileDB does not enforce the
precision or scale of the decimal types.

## Create Table

Create table is supported, however only a limited subset of TileDB parameters
are supported.

-   No support for creating encrypted arrays
-   No support for setting custom filters on attributes, coordinate or offsets


## Splits

The current split implementation is naive and splits domains evenly 
with user defined predicates (where clause) or from the non-empty domains.
This even splitting will likely produce sub optimal splits for sparse
domains. Future work will move splitting into core TileDB where better
heuristics will be used to produce even splits.

For now if splits are highly uneven consider increasing the number of splits
via the `tiledb.splits` session parameter or add where clauses to limit the
data set to non-empty regions of the array.