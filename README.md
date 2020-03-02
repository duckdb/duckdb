<img align="left" src="logo/duckdb-logo.png" height="120">

# DuckDB, the SQLite for Analytics
[![Travis](https://api.travis-ci.org/cwida/duckdb.svg?branch=master)](https://travis-ci.org/cwida/duckdb)
[![CodeFactor](https://www.codefactor.io/repository/github/cwida/duckdb/badge)](https://www.codefactor.io/repository/github/cwida/duckdb)
[![Coverage Status](https://coveralls.io/repos/github/cwida/duckdb/badge.svg?branch=master)](https://coveralls.io/github/cwida/duckdb?branch=master)

<br>


# Requirements
DuckDB requires [CMake](https://cmake.org) to be installed and a `C++11` compliant compiler. GCC 4.9 and newer, Clang 3.9 and newer and VisualStudio 2017 are tested on each revision.

## Compiling
Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized debug version. You may run `make unit` and `make allunit` to verify that your version works properly after making changes.

# Usage
A command line utility based on `sqlite3` can be found in either `build/release/duckdb_cli` (release, the default) or `build/debug/duckdb_cli` (debug).

# Embedding
As DuckDB is an embedded database, there is no database server to launch or client to connect to a running server. However, the database server can be embedded directly into an application using the C or C++ bindings. The main build process creates the shared library `build/release/src/libduckdb.[so|dylib|dll]` that can be linked against. A static library is built as well.

For examples on how to embed DuckDB into your application, see the [examples](https://github.com/cwida/duckdb/tree/master/examples) folder.

## Benchmarks
After compiling, benchmarks can be executed from the root directory by executing `./build/release/benchmark/benchmark_runner`.

## Standing on the Shoulders of Giants
DuckDB is implemented in C++ 11, should compile with GCC and clang, uses CMake to build and [Catch2](https://github.com/catchorg/Catch2) for testing. DuckDB uses some components from various Open-Source databases and draws inspiration from scientific publications. Here is an overview:

* Parser: We use the PostgreSQL parser that was [repackaged as a stand-alone library](https://github.com/lfittl/libpg_query). The translation to our own parse tree is inspired by [Peloton](https://pelotondb.io).
* Shell: We have adapted the [SQLite shell](https://sqlite.org/cli.html) to work with DuckDB.
* Tests: We use the [SQL Logic Tests from SQLite](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) to test DuckDB.
* Query fuzzing: We use [SQLsmith](https://github.com/anse1/sqlsmith) to generate random queries for additional testing.
* Date Math: We use the date math component from [MonetDB](https://www.monetdb.org).
* SQL Window Functions: DuckDB's window functions implementation uses Segment Tree Aggregation as described in the paper "Efficient Processing of Window Functions in Analytical SQL Queries" by Viktor Leis, Kan Kundhikanjana, Alfons Kemper and Thomas Neumann.
* Execution engine: The vectorized execution engine is inspired by the paper "MonetDB/X100: Hyper-Pipelining Query Execution" by Peter Boncz, Marcin Zukowski and Niels Nes.
* Optimizer: DuckDB's optimizer draws inspiration from the papers "Dynamic programming strikes back" by Guido Moerkotte and Thomas Neumman as well as "Unnesting Arbitrary Queries" by Thomas Neumann and Alfons Kemper.
* Concurrency control: Our MVCC implementation is inspired by the paper "Fast Serializable Multi-Version Concurrency Control for Main-Memory Database Systems" by Thomas Neumann, Tobias Mühlbauer and Alfons Kemper.
* Regular Expression: DuckDB uses Google's [RE2](https://github.com/google/re2) regular expression engine.

## Other pages
* [Continuous Benchmarking (CB™)](https://www.duckdb.org/benchmarks/index.html), runs TPC-H, TPC-DS and some microbenchmarks on every commit
