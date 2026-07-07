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

The feature store extension adds SQL statements for defining, refreshing, and serving ML features:

```sql
-- Define a feature over a source (event) table. The GROUP BY keys (the entity
-- columns) must be a FOREIGN KEY into the declared ENTITY table.
CREATE FEATURE user_activity
    ENTITY users
    TIMESTAMP event_time
    WINDOW 24 HOURS
    EVERY 1 HOUR
    RETAIN 3
    AS (SELECT user_id, COUNT(*) AS event_count, AVG(region_id) AS avg_region
        FROM events GROUP BY user_id);

-- Recompute the feature (as of now, or as of a specific point in time) and
-- append the result as a new version to the backing store
REFRESH FEATURE user_activity;
REFRESH FEATURE user_activity AT '2024-01-04 00:00:00';

-- Toggle or change the automatic refresh schedule
ALTER FEATURE user_activity DISABLE SCHEDULE;
ALTER FEATURE user_activity SET SCHEDULE EVERY 30 MINUTES;

-- Point-in-time correct retrieval via ASOF join against a spine table
SERVE FEATURE user_activity FOR spine_table;
SERVE FEATURE user_activity FOR spine_table ENTITY spine_user_id ASOF request_time;

-- Remove the feature (and its owned resolver view)
DROP FEATURE user_activity;
```

`CREATE FEATURE` only registers metadata — no data is materialized until the first `REFRESH`. Every `REFRESH FEATURE` fully recomputes the `WINDOW` aggregate over the source table and appends the result as a new version to a single denormalized backing table (`feature_name__store`), tagging each row with an internal version and timestamp. A view named `feature_name` always resolves to the current version; `feature_at_version(name, version)` reads an explicit past version; `duckdb_features()` exposes feature metadata. Old versions are garbage-collected automatically according to `RETAIN N`. `SERVE FEATURE` performs an ASOF join against the backing store for point-in-time correct retrieval, with `ENTITY`/`ASOF` optionally overriding the join columns. A feature can be scheduled to refresh automatically with `EVERY <interval>`, and the schedule can be changed or toggled later with `ALTER FEATURE`.

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
