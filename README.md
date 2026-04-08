<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/DuckDB_Logo-horizontal.svg">
    <source media="(prefers-color-scheme: dark)" srcset="logo/DuckDB_Logo-horizontal-dark-mode.svg">
    <img alt="DuckDB logo" src="logo/DuckDB_Logo-horizontal.svg" height="100">
  </picture>
</div>
<br>

<p align="center">
  <a href="https://github.com/duckdb/duckdb/actions"><img src="https://github.com/duckdb/duckdb/actions/workflows/Main.yml/badge.svg?branch=main" alt="Github Actions Badge"></a>
  <a href="https://discord.gg/tcvwpjfnZx"><img src="https://shields.io/discord/909674491309850675" alt="discord" /></a>
  <a href="https://github.com/duckdb/duckdb/releases/"><img src="https://img.shields.io/github/v/release/duckdb/duckdb?color=brightgreen&display_name=tag&logo=duckdb&logoColor=white" alt="Latest Release"></a>
</p>

## DuckDB

DuckDB is a high-performance analytical database system. It is designed to be fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs, maps), and [several extensions designed to make SQL easier to use](https://duckdb.org/docs/current/sql/dialect/friendly_sql.html).

DuckDB is available as a [standalone CLI application](https://duckdb.org/docs/current/clients/cli/overview) and has clients for [Python](https://duckdb.org/docs/current/clients/python/overview), [R](https://duckdb.org/docs/current/clients/r), [Java](https://duckdb.org/docs/current/clients/java), [Wasm](https://duckdb.org/docs/current/clients/wasm/overview), etc., with deep integrations with packages such as [pandas](https://duckdb.org/docs/guides/python/sql_on_pandas) and [dplyr](https://duckdb.org/docs/current/clients/r#duckplyr-dplyr-api).

For more information on using DuckDB, please refer to the [DuckDB documentation](https://duckdb.org/docs/current/).

## Installation

If you want to install DuckDB, please see [our installation page](https://duckdb.org/docs/installation/) for instructions.

## Data Import

For CSV files and Parquet files, data import is as simple as referencing the file in the FROM clause:

```sql
SELECT * FROM 'myfile.csv';
SELECT * FROM 'myfile.parquet';
```

Refer to our [Data Import](https://duckdb.org/docs/current/data/overview) section for more information.

## SQL Reference

The documentation contains a [SQL introduction and reference](https://duckdb.org/docs/current/sql/introduction).

## Development

DuckDB development requires [CMake](https://cmake.org), Python 3 and a `C++17` compliant compiler.

Common commands from the repository root:

- `make`: build the default release binaries
- `make debug`: build a non-optimized debug version
- `make unit`: run fast unit tests
- `make allunit`: run the full unit test suite
- `BUILD_BENCHMARK=1 BUILD_TPCH=1 make`: build benchmark binaries
- `./build/release/benchmark/benchmark_runner`: run standard benchmarks

Benchmark details are in the [Benchmark Guide](benchmark/README.md).

Please also refer to our [Build Guide](https://duckdb.org/docs/current/dev/building/overview) and [Contribution Guide](CONTRIBUTING.md).

## Repository Layout

- `src/`: core DuckDB engine
- `test/`: SQL logic tests and C++ unit tests
- `extension/`: built-in and loadable extensions
- `benchmark/`: benchmark suites and tools

## Support

See the [Support Options](https://duckdblabs.com/support/) page and the dedicated [`endoflife.date`](https://endoflife.date/duckdb) page.
