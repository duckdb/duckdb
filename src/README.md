This folder contains the source tree of the core of DuckDB. Below is a short overview of the different components.

# Parser
The parser can be found in the [parser](https://github.com/cwida/duckdb/tree/master/src/parser) folder. This is the entry point for any query that enters DuckDB. DuckDB uses the parser of Postgres ([libpg_query](https://github.com/lfittl/libpg_query)). After parsing the query using that parser, the tokens are transformed into a custom parse tree representation that is based on `SQLStatements`, `Expressions` and `TableRefs`. For more information, see the [README](https://github.com/cwida/duckdb/tree/master/src/parser) in the parser folder.

# Planner
