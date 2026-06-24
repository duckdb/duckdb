<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/DuckDB_Logo-horizontal.svg">
    <source media="(prefers-color-scheme: dark)" srcset="logo/DuckDB_Logo-horizontal-dark-mode.svg">
    <img alt="DuckDB logo" src="logo/DuckDB_Logo-horizontal.svg" height="100">
  </picture>
</div>
<br>

## DuckDB with Feature Stores

This is a fork of DuckDB extended with a native **feature store** — a first-class SQL interface for defining, materialising, and serving ML features directly inside the database.

DuckDB is a high-performance analytical database system. It is designed to be fast, reliable, portable, and easy to use. DuckDB provides a rich SQL dialect with support far beyond basic SQL. DuckDB supports arbitrary and nested correlated subqueries, window functions, collations, complex types (arrays, structs, maps), and [several extensions designed to make SQL easier to use](https://duckdb.org/docs/current/sql/dialect/friendly_sql.html).

## Feature Store

The feature store extension adds four SQL statements for managing ML features:

```sql
-- Define a feature over a source table
CREATE FEATURE user_activity ON hits
    ENTITY UserID
    TIMESTAMP EventTime
    GRANULARITY HOUR
    WINDOW 24
    REFRESH FULL
    RETAIN 1
    AS (SELECT UserID, COUNT(*) AS event_count, AVG(RegionID) AS avg_region);

-- Recompute/update the feature backing table
REFRESH FEATURE user_activity;

-- Point-in-time correct retrieval via ASOF join
SERVE FEATURE user_activity FOR spine_table;

-- Remove the feature and all its version tables
DROP FEATURE user_activity;
```

Each `REFRESH FEATURE` creates a new versioned backing table (`feature_name__v1`, `feature_name__v2`, …). A view named `feature_name` always resolves to the current version. Old versions are garbage-collected automatically according to `RETAIN N`.

Both `FULL` (full recompute) and `INCREMENTAL` (watermark-based) refresh modes are supported.

## Development

DuckDB requires [CMake](https://cmake.org), Python 3 and a `C++17` compliant compiler.

### Building

To build with feature store and HTTP filesystem support (recommended):

```bash
BUILD_BENCHMARK=1 BUILD_HTTPFS=1 make reldebug
```

If the build runs out of memory, reduce parallelism:

```bash
CMAKE_BUILD_PARALLEL_LEVEL=2 BUILD_BENCHMARK=1 BUILD_HTTPFS=1 make reldebug
```

For a debug build (slower, with sanitizers and assertions):

```bash
make debug
```

### Testing

To access shell environment:
```bash
build/reldebug/duckdb
```

Run the feature store SQL tests:

```bash
build/reldebug/test/unittest "test/sql/feature/*"
```

Run all unit tests:

```bash
build/reldebug/test/unittest "*"
```

### Benchmarks

Run the feature store benchmarks (requires ClickBench data, downloaded automatically on first run):

```bash
build/reldebug/benchmark/benchmark_runner "benchmark/feature/.*"
```

Run standard DuckDB benchmarks:

```bash
build/reldebug/benchmark/benchmark_runner
```

See [Benchmark Guide](benchmark/README.md) for details.

Please also refer to our [Build Guide](https://duckdb.org/docs/current/dev/building/overview) and [Contribution Guide](CONTRIBUTING.md).
