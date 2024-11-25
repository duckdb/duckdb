# DuckDB-PGQ

This is a forked repository of [DuckDB](https://github.com/duckdb/duckdb) to support the [DuckPGQ](https://github.com/cwida/duckpgq-extension) extension.
Do not clone this repository directly to use the DuckPGQ extension.
To build the extension from source, see the [DuckPGQ](https://github.com/cwida/duckpgq-extension) repository 
and the [documentation page](https://duckpgq.notion.site/duckpgq/b8ac652667964f958bfada1c3e53f1bb?v=3b47a8d44bdf4e0c8b503bf23f1b76f2) for instructions.

[![Discord](https://discordapp.com/api/guilds/1225369321077866496/widget.png?style=banner3)](https://discord.gg/8X95XHhQB7)

# Loading DuckPGQ into DuckDB
As of DuckDB v1.1.* we support loading DuckPGQ as a community extension. 
```sql
install duckpgq from community; 
load duckpgq;
```


For availability please see the [DuckPGQ extension availability section](https://github.com/cwida/duckpgq-extension#duckpgq-extension-availability).

Since this is a third-party extension, DuckDB must be started in `unsigned` mode to load it. The extension can be loaded with the following commands: 

For CLI:
```bash
duckdb -unsigned

set custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';
force install 'duckpgq'; # ensures any existing DuckPGQ version already installed is overwritten
load 'duckpgq';
```

For Python:
```python
import duckdb 
conn = duckdb.connect(config = {"allow_unsigned_extensions": "true"})

conn.execute("set custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';")
conn.execute("force install 'duckpgq';")
conn.execute("load 'duckpgq';")
```
## SQL Reference

The documentation contains a [SQL introduction and reference](https://duckdb.org/docs/sql/introduction).

## Development

For development, DuckDB requires [CMake](https://cmake.org), Python3 and a `C++11` compliant compiler. Run `make` in the root directory to compile the sources. For development, use `make debug` to build a non-optimized debug version. You should run `make unit` and `make allunit` to verify that your version works properly after making changes. To test performance, you can run `BUILD_BENCHMARK=1 BUILD_TPCH=1 make` and then perform several standard benchmarks from the root directory by executing `./build/release/benchmark/benchmark_runner`. The details of benchmarks are in our [Benchmark Guide](benchmark/README.md).
