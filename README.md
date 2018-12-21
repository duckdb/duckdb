# DuckDB, the SQLite for Analytics
[![Build Status](http://jenkins.u0d.de/buildStatus/icon?job=duckdb)](http://jenkins.u0d.de/job/duckdb) [![Coverage](http://www.duckdb.org/coverage/badge.svg)](http://www.duckdb.org/coverage/) 
[![CodeFactor](https://www.codefactor.io/repository/github/cwida/duckdb/badge)](https://www.codefactor.io/repository/github/cwida/duckdb)

# Requirements
DuckDB requires [CMake](https://cmake.org) to be installed and a `C++11` compliant compiler. To run all tests, a `git` installation is also required.

## Compiling
Run `make` in the root directory to compile the sources into a debug version. Use `make opt` to create a release build, which is *critical to get best performance*. Once compilation is done, you may run `make unit` and `make allunit` to verify that your version works properly.

# Usage
A command line utility based on `sqlite3` can be found in either `build/debug/tools/shell/shell` (debug) or `build/release/tools/shell/shell` (release).

# Embedding
As DuckDB is an embedded database, there is no database server to launch or client to connect to a running server. However, the database server can be embedded directly into an application using the C or C++ bindings. The main build process creates the shared library `build/release/src/libduckdb.[so|dylib|dll]` that can be linked against. A static library can be build by changing the `SHARED` text in `src/CMakeLists.txt` to `STATIC`.

For examples on how to embed DuckDB into your application, see the [examples](https://github.com/cwida/duckdb/tree/master/examples) folder.

## Standing on the Shoulders of Giants
DuckDB is implemented in C++ 11, should compile with GCC and clang, uses CMake to build and [Catch2](https://github.com/catchorg/Catch2) for testing. In addition, we use [Jenkins](https://jenkins.io) as a CI platform. DuckDB uses some components from various Open-Source databases and draws inspiration from scientific publications. Here is an overview:

* Parser: We use the PostgreSQL parser that was [repackaged as a stand-alone library](https://github.com/lfittl/libpg_query). The translation to our own parse tree is inspired by [Peloton](https://pelotondb.io), as is the parse tree structure itself.
* Shell: We have adapted the [SQLite shell](https://sqlite.org/cli.html) to work with DuckDB.
* Tests: We use the [SQL Logic Tests from SQLite](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) to test DuckDB.
* Query fuzzing: We use [SQLsmith](https://github.com/anse1/sqlsmith) to generate random queries for additional testing.
* Date Math: We use the date math component from [MonetDB](https://www.monetdb.org).
* Execution engine: The vectorized execution engine is inspired by the paper "MonetDB/X100: Hyper-Pipelining Query Execution" by Peter Boncz, Marcin Zukowski and Niels Nes.
* Concurrency control: Our MVCC implementation is inspired by the paper "Fast Serializable Multi-Version Concurrency Control for Main-Memory Database Systems" by Thomas Neumann, Tobias Mühlbauer and Alfons Kemper.
* Rule-based Rewriting: The query rewriting rule engine is inspired by the one in [Apache Calcite](https://calcite.apache.org).

## Other pages
* [Continous Benchmarking (CB™)](http://www.duckdb.org/benchmarking/), runs TPC-H, TPC-DS and some microbenchmarks on every commit
