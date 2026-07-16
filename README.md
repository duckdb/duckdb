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

The feature store extension adds first-class SQL statements for **defining**, **materialising**, **serving**, and **inspecting** ML features. Below is the full supported syntax, followed by how it works and a worked example.

### `CREATE FEATURE` — define a feature

```sql
CREATE FEATURE [IF NOT EXISTS] <name>
    ENTITY <entity_table> [(<key_col>, ...)]   -- the entity dimension (one snapshot row per entity)
    TIMESTAMP <ts_col>                          -- event-time column used for the lookback window
    [WINDOW <interval>]                         -- lookback window for the aggregate (e.g. 24 HOURS)
    [TTL <interval>]                            -- serving staleness bound (see SERVE below)
    [EVERY <interval>]                          -- attach an automatic refresh schedule
    [RETAIN <n>]                                -- how many versions to keep (default 1)
    AS (<select_query>);                        -- the aggregate that produces feature values
```

`CREATE FEATURE` only registers **metadata** — no data is materialised until the first `REFRESH`. The entity keys are resolved in this order: the explicit `ENTITY t (cols)` list if given, else the entity table's `PRIMARY KEY`, else whichever entity-table columns the query projects. If the query projects **no** entity column, the feature is *global* (a single aggregate row). Intervals accept `INTERVAL '2 days'`, `INTERVAL 2 DAYS`, or the `2 DAYS` shorthand (units: `SECOND(S)`, `MINUTE(S)`, `HOUR(S)`, `DAY(S)`).

### `REFRESH FEATURE` — materialise a version

```sql
REFRESH FEATURE <name>;                          -- snapshot as of now
REFRESH FEATURE <name> AT '2024-01-04 00:00:00'; -- snapshot as of a point in time
```

Each `REFRESH` recomputes the `WINDOW` aggregate over the source table and appends the result as a **new version** to the single denormalised backing table `<name>__store`, tagging every row with an internal version and timestamp. Versions outside `RETAIN <n>` are garbage-collected in place.

### `ALTER FEATURE` — change schedule or TTL

```sql
ALTER FEATURE [IF EXISTS] <name> SET SCHEDULE EVERY <interval>;  -- attach/replace schedule (and enable)
ALTER FEATURE [IF EXISTS] <name> ENABLE SCHEDULE;               -- re-enable, interval unchanged
ALTER FEATURE [IF EXISTS] <name> DISABLE SCHEDULE;             -- keep interval, stop firing
ALTER FEATURE [IF EXISTS] <name> SET TTL <interval>;          -- change the serving staleness bound
```

All `ALTER FEATURE` changes are transactional and survive a restart (written to the WAL / checkpoint).

### Reading a feature

```sql
-- The current version resolves by using the feature name as a relation:
SELECT * FROM <name>;

-- A specific retained version (only available while inside the RETAIN window):
SELECT * FROM FEATURE <name> AT VERSION 3;

-- Feature metadata (window, ttl, schedule, current_version, retain_versions, ...):
SELECT * FROM duckdb_features();
```

### `SERVE FEATURE` — point-in-time correct retrieval

```sql
-- Join one or more features onto a spine table via a LEFT ASOF join:
SERVE FEATURE  <name> FOR <spine>;
SERVE FEATURES <name1>, <name2> FOR <spine>;

-- Override the join columns when the spine's names differ from the feature's:
SERVE FEATURE <name> FOR <spine> ENTITY <spine_entity_col> ASOF <spine_ts_col>;

-- Map differently-named entity keys explicitly (feature_key = spine_key):
SERVE FEATURE <name> ENTITY (card_no = customer_id) FOR <spine> ASOF request_time;
```

`SERVE` matches each spine row to the entity's latest snapshot **at or before** the spine timestamp (an ASOF join), so there is no label leakage. Unmatched spine rows are kept with `NULL` feature values (LEFT join). If a `TTL` is set, a matched snapshot older than the spine timestamp by more than the TTL is served as `NULL` (a staleness guard); a zero/absent TTL never nulls on age.

### `DROP FEATURE`

```sql
DROP FEATURE [IF EXISTS] <name>;   -- removes the feature, its backing store, and metadata
```

### Worked example

```sql
CREATE TABLE users (user_id VARCHAR);
CREATE TABLE events (user_id VARCHAR, region_id INTEGER, event_time TIMESTAMP);

-- Define a feature. The GROUP BY keys (the entity columns) map onto the ENTITY table.
CREATE FEATURE user_activity
    ENTITY users
    TIMESTAMP event_time
    WINDOW 24 HOURS
    TTL 7 DAYS
    EVERY 1 HOUR
    RETAIN 3
    AS (SELECT user_id, COUNT(*) AS event_count, AVG(region_id) AS avg_region
        FROM events GROUP BY user_id);

-- Materialise version 1 (as of a fixed time), then read it back.
REFRESH FEATURE user_activity AT '2024-01-04 00:00:00';
SELECT * FROM user_activity;                       -- current version
SELECT * FROM FEATURE user_activity AT VERSION 1;  -- an explicit version

-- Change the schedule and the TTL later on.
ALTER FEATURE user_activity SET SCHEDULE EVERY 30 MINUTES;
ALTER FEATURE user_activity SET TTL 2 DAYS;

-- Point-in-time serving against a spine of (entity, request_time) rows.
SELECT * FROM (SELECT 'alice' AS user_id, TIMESTAMP '2024-01-04 06:00:00' AS request_time) spine;
SERVE FEATURE user_activity FOR spine ASOF request_time;

DROP FEATURE user_activity;
```

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

The feature store benchmarks live under `benchmark/feature/clickstream/` in three families —
`serve/` (point-in-time SERVE), `refresh/` (windowed REFRESH snapshots) and `cases/` (multi-step
workflows). They require ClickBench data, downloaded automatically on first run and cached per scale.

```bash
# run everything under the feature group
build/reldebug/benchmark/benchmark_runner "benchmark/feature/.*"

# or a single family
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/serve/serve_.*"
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/refresh/refresh_.*"
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/cases/case_.*"

# list without running
build/reldebug/benchmark/benchmark_runner --list | grep clickstream/
```

The scenarios are generated (not hand-written) by a seeded pairwise generator; see
[benchmark/feature/clickstream/README.md](benchmark/feature/clickstream/README.md) for the methodology
and how to regenerate them.

Run standard DuckDB benchmarks:

```bash
build/reldebug/benchmark/benchmark_runner
```

See [Benchmark Guide](benchmark/README.md) for details.

Please also refer to our [Build Guide](https://duckdb.org/docs/current/dev/building/overview) and [Contribution Guide](CONTRIBUTING.md).
