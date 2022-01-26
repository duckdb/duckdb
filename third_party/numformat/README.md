# Modified OpenOffice number format parser

This directory holds the core number format parser that is used by DuckDB. It is based on the [OpenOffice number format parser](https://github.com/apache/openoffice/tree/trunk/main/svl/source/numbers), but has been stripped down and generally cleaned up to make it easier to modify and extend.

The most important changes made are listed here:
* The parser and all its auxiliary structures are wrapped in the `duckdb_numformat` namespace.
* Duplication is reduced and code is simplified by e.g. not requiring the same keyword or statement to be declared in multiple different places. 
* All codes used in the source code is corresponded to Unicode. 
