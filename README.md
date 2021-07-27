<img src="https://duckdb.org/images/DuckDB_Logo_dl.png" height="50">

![.github/workflows/main.yml](https://github.com/cwida/duckdb/workflows/.github/workflows/main.yml/badge.svg?branch=master)
[![CodeFactor](https://www.codefactor.io/repository/github/cwida/duckdb/badge)](https://www.codefactor.io/repository/github/cwida/duckdb)
[![codecov](https://codecov.io/gh/duckdb/duckdb/branch/master/graph/badge.svg?token=FaxjcfFghN)](https://codecov.io/gh/duckdb/duckdb)


## Installation
If you just want to install and use DuckDB, please see [our website](https://www.duckdb.org) for installation and usage instructions.

## Development 
For development, DuckDB requires [CMake](https://cmake.org), Python3 and a `C++11` compliant compiler. Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized debug version. You should run `make unit` and `make allunit` to verify that your version works properly after making changes. To test performance, you can run several standard benchmarks from the root directory by executing `./build/release/benchmark/benchmark_runner`.

Please also refer to our [Contribution Guide](CONTRIBUTING.md).


