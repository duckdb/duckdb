# Official DuckDB Julia Package

DuckDB is a high-performance in-process analytical database system. It is designed to be fast, reliable and easy to use. For more information on the goals of DuckDB, please refer to [the Why DuckDB page on our website](https://duckdb.org/why_duckdb).

The DuckDB Julia package provides a high-performance front-end for DuckDB. Much like SQLite, DuckDB runs in-process within the Julia client, and provides a DBInterface front-end.

The package also supports multi-threaded execution. It uses Julia threads/tasks for this purpose. If you wish to run queries in parallel, you must launch Julia with multi-threading support (by e.g. setting the `JULIA_NUM_THREADS` environment variable).  


## Installation

```julia
pkg> add DuckDB

julia> using DuckDB
```

## Basics

```julia
# create a new in-memory database
con = DBInterface.connect(DuckDB.DB, ":memory:")

# create a table
DBInterface.execute(con, "CREATE TABLE integers(i INTEGER)")

# insert data using a prepared statement
stmt = DBInterface.prepare(con, "INSERT INTO integers VALUES(?)")
DBInterface.execute(stmt, [42])

# query the database
results = DBInterface.execute(con, "SELECT 42 a")
print(results)
```

## Scanning DataFrames
The DuckDB Julia package also provides support for querying Julia DataFrames. Note that the DataFrames are directly read by DuckDB - they are not inserted or copied into the database itself.

If you wish to load data from a DataFrame into a DuckDB table you can run a `CREATE TABLE AS` or `INSERT INTO` query.

```julia
using DuckDB
using DataFrames

# create a new in-memory dabase
con = DBInterface.connect(DuckDB.DB)

# create a DataFrame
df = DataFrame(a = [1, 2, 3], b = [42, 84, 42])

# register it as a view in the database
DuckDB.register_data_frame(con, df, "my_df")

# run a SQL query over the DataFrame
results = DBInterface.execute(con, "SELECT * FROM my_df")
print(results)
```

## Original Julia Connector
Credits to kimmolinna for the [original DuckDB Julia connector](https://github.com/kimmolinna/DuckDB.jl).

## Contributing to the Julia Package

### Formatting
The format script must be run when changing anything. This can be done by running the following command from within the root directory of the project:

```bash
julia tools/juliapkg/scripts/format.jl
```

### Testing

You can run the tests using the `test.sh` script:

```
./test.sh
```

Specific test files can be run by adding the name of the file as an argument:

```
./test.sh test_connection.jl
```

In order to run against a locally compiled version of DuckDB, you will can set the `JULIA_DUCKDB_LIBRARY` environment variable, e.g.:

```bash
export JULIA_DUCKDB_LIBRARY="`pwd`/../../build/debug/src/libduckdb.dylib"
```

Note that Julia pre-compilation caching might get in the way of changes to this variable taking effect. You can clear these caches using the following command:

```bash
rm -rf ~/.julia/compiled
```

### Submitting a New Package
The DuckDB Julia package depends on the [DuckDB_jll package](https://github.com/JuliaBinaryWrappers/DuckDB_jll.jl), which can be updated by sending a PR to [Yggdrassil](https://github.com/JuliaPackaging/Yggdrasil/pull/5049).

After the `DuckDB_jll` package is updated, the DuckDB package can be updated by incrementing the version number (and dependency version numbers) in `Project.toml`, followed by [adding a comment containing the text `@JuliaRegistrator register subdir=tools/juliapkg`](https://github.com/duckdb/duckdb/commit/88b59799f41fce7cbe166e5c33d0d5f6d480278d#commitcomment-76533721) to the commit. 
