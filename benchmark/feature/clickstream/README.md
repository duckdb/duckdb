# Clickstream feature-store benchmarks

These benchmarks exercise the feature-store SQL surface (`CREATE FEATURE`, `REFRESH [AT]`,
`SERVE ... [ASOF]`) over a ClickBench-derived "hits" clickstream. Instead of hand-writing one
benchmark per pinned scenario, each benchmark is a **point in a knob space**, and the space is
sampled with a seeded, coverage-guaranteed generator.

## Layout

```
clickstream/
  serve/     serve.benchmark.in    + serve_*.benchmark     # time SERVE (ASOF retrieval)   [pairwise]
  refresh/   refresh.benchmark.in  + refresh_*.benchmark   # time REFRESH (snapshot)       [pairwise]
  cases/     cases.benchmark.in    + case_*.benchmark      # multi-step scenarios          [curated]
  scenarios.manifest       # seed + per-family dimensions + the exact list of sampled vectors
  README.md
```

Each family is a shared, parameterized `.benchmark.in` template plus thin generated stubs (one per
scenario). All three reuse the same mechanism: a `template` block injects `BENCH_NAME`,
`PARQUET_COUNT`, `SETUP_SQL` (untimed feature setup) and `RUN_QUERY` (the timed statement).

- **serve/** — timed statement is a `SELECT ... FROM SERVE FEATURE ...`; pairwise over 10 knobs.
- **refresh/** — timed statement is a single `REFRESH FEATURE f AT ...` (WINDOW is a live knob here);
  pairwise over 5 knobs.
- **cases/** — curated multi-step scenarios whose `RUN_QUERY` is a multi-statement sequence on one
  line (interleaved refresh+serve, GC pressure, concurrent multi-feature refresh, refresh-then-serve),
  emitted at each source scale. These set `require_reinit` so each hot run starts from a clean,
  freshly-rebuilt feature store (the timed sequence mutates state).

## Why a scenario space?

Hand-pinned benchmarks only measure the corners someone thought to type, and they encode author bias
about what is slow. The feature-store cost surface has many orthogonal knobs — source scale,
aggregation complexity, version depth, retention/GC volume, serve mode, serve-timestamp position,
TTL, feature count, and query wrapper — whose _interactions_ are where surprises hide. We make each
knob a template variable and let a generator sample vectors across the whole space.

Dimensions and constraints are declared in
[scripts/generate_feature_benchmarks.py](../../../scripts/generate_feature_benchmarks.py) and echoed
into [scenarios.manifest](scenarios.manifest).

## The "random list": seeded pairwise sampling

Full cross-product of the knobs is thousands of runs. Instead the default strategy builds a
**pairwise covering array**: the emitted set provably contains every _pair_ of knob values (that can
co-occur) across any two dimensions at least once. This is combinatorial interaction testing — the
approach used in randomized DB testing research — and it catches most interaction bugs in a fraction
of the runs (here: serve = 21 scenarios covering all 345 value-pairs; refresh = 14 scenarios covering
all 77 value-pairs). The `cases/` family is curated rather than sampled, since those are specific
hand-chosen workflows.

Everything is driven by `--seed`, so the "random list" is fully reproducible: the same seed emits
byte-identical stubs and manifest. Other strategies are available for comparison
(`--strategy random` = naive independent draws; `--strategy grid` = exhaustive).

The randomness lives in the **generator**, not in SQL: the benchmark runner has no loop construct
(one `.benchmark` file = one timed, separately-reported benchmark), so distinct comparable runs must
be distinct files. (SQL-side `random()` would only jitter data and perturb timings non-reproducibly.)

## Regenerating

```bash
python3 scripts/generate_feature_benchmarks.py                 # regenerate the checked-in set (seed 42)
python3 scripts/generate_feature_benchmarks.py --seed 7        # a different reproducible list
python3 scripts/generate_feature_benchmarks.py --verify        # assert full pairwise coverage
python3 scripts/generate_feature_benchmarks.py --strategy random --count 12
```

## Running

```bash
# a whole family (downloads the ClickBench source once per scale, then caches it)
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/serve/serve_.*"
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/refresh/refresh_.*"
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/cases/case_.*"
# just list / inspect without executing
build/reldebug/benchmark/benchmark_runner --list | grep clickstream/
```

All families share the per-scale source cache (`feature_src_<N>.db`), so once one family has
downloaded a scale, the others reuse it.

## How the template wires the knobs (implementation notes)

- **Substitution reaches inline template lines and the `load`/`cache` filenames, but not the contents
  of an external `load file.sql`** (that file is read raw). So the source build and the
  scenario-specific feature setup are **inline** in `serve.benchmark.in`, where `${VAR}` expands.
- **The source is cached per scale** via `cache feature_src_${PARQUET_COUNT}.db`; the key must include
  the scale or the 20-file and 100-file datasets would collide.
- **Feature setup is idempotent and split across `load`/`reload`.** The runner runs `init` _before_
  `load`, so feature DDL cannot live in `init` (the source tables don't exist yet). Instead the
  first run per scale builds source + features in `load` (and caches the source); subsequent runs
  rebuild only the scenario's features via `reload` (which runs when the cache already exists). Both
  start with `DROP FEATURE IF EXISTS ...` so re-runs and scenario switches on a shared cache are clean.
- **Only the `run` block is timed.** For `serve/` the timed statement is read-only, so hot iterations
  don't accumulate versions and setup happens once. For `refresh/`/`cases/` the timed statement
  mutates (REFRESH appends versions); `cases/` re-runs `SETUP_SQL` in an (untimed) `cleanup` block so
  each hot run starts from a clean feature store, and `refresh/` measures the steady-state refresh
  (bounded by `RETAIN`).
- **`cases/` uses `cleanup`, not `require_reinit`.** Both give a clean store per run, but
  `require_reinit` reopens the whole cached database before every hot run — a full close + checkpoint
  + reopen of a multi-GB file. That is ~14s of untimed work per run at s20 (and minutes at s100)
  against ~1s of measurement, and because the runner prints the run number *before* re-initialising,
  the wait looks like the next run hanging. Re-running the idempotent `SETUP_SQL` in `cleanup`
  reaches the same state ~6x faster. The trailing `CHECKPOINT` matters: it reclaims the blocks freed
  by `DROP FEATURE`, without which the cache grows ~1GB per run and timings drift slower.
- A lightweight `assert` (source populated; for `serve/`, store materialized and spine non-empty)
  guards against degenerate empty-state timings; exact result values are not checked because they
  vary per scenario.
