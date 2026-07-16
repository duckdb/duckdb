# Clickstream feature-store benchmarks

These benchmarks exercise the feature-store SQL surface (`CREATE FEATURE`, `REFRESH [AT]`,
`SERVE ... [ASOF]`) over a ClickBench-derived "hits" clickstream. Instead of hand-writing one
benchmark per pinned scenario, each benchmark is a **point in a knob space**, and the space is
sampled with a seeded, coverage-guaranteed generator.

## Layout

```
clickstream/
  serve/
    serve.benchmark.in     # shared, parameterized template (one .in per family)
    serve_*.benchmark      # generated stubs: one per sampled knob vector
  scenarios.manifest       # seed + strategy + the exact list of sampled vectors
  README.md
```

`refresh/` and `cases/` are the follow-up families (not part of this proof-of-concept). The generator
and template pattern generalize to them directly.

## Why a scenario space?

Hand-pinned benchmarks only measure the corners someone thought to type, and they encode author bias
about what is slow. The feature-store cost surface has many orthogonal knobs — source scale,
aggregation complexity, version depth, retention/GC volume, serve mode, serve-timestamp position,
TTL, feature count, and query wrapper — whose *interactions* are where surprises hide. We make each
knob a template variable and let a generator sample vectors across the whole space.

Dimensions and constraints are declared in
[scripts/generate_feature_benchmarks.py](../../../scripts/generate_feature_benchmarks.py) and echoed
into [scenarios.manifest](scenarios.manifest).

## The "random list": seeded pairwise sampling

Full cross-product of the knobs is thousands of runs. Instead the default strategy builds a
**pairwise covering array**: the emitted set provably contains every *pair* of knob values (that can
co-occur) across any two dimensions at least once. This is combinatorial interaction testing — the
approach used in randomized DB testing research — and it catches most interaction bugs in a fraction
of the runs (here: 21 scenarios cover all 345 achievable value-pairs).

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
# all clickstream serve scenarios (downloads the ClickBench source once per scale, then caches it)
build/reldebug/benchmark/benchmark_runner "benchmark/feature/clickstream/serve/serve_.*"
# just list / inspect without executing
build/reldebug/benchmark/benchmark_runner --list | grep clickstream/serve
```

## How the template wires the knobs (implementation notes)

- **Substitution reaches inline template lines and the `load`/`cache` filenames, but not the contents
  of an external `load file.sql`** (that file is read raw). So the source build and the
  scenario-specific feature setup are **inline** in `serve.benchmark.in`, where `${VAR}` expands.
- **The source is cached per scale** via `cache feature_src_${PARQUET_COUNT}.db`; the key must include
  the scale or the 20-file and 100-file datasets would collide.
- **Feature setup is idempotent and split across `load`/`reload`.** The runner runs `init` *before*
  `load`, so feature DDL cannot live in `init` (the source tables don't exist yet). Instead the
  first run per scale builds source + features in `load` (and caches the source); subsequent runs
  rebuild only the scenario's features via `reload` (which runs when the cache already exists). Both
  start with `DROP FEATURE IF EXISTS ...` so re-runs and scenario switches on a shared cache are clean.
- **`SERVE` is the only timed statement** (`run`); it is read-only, so repeated hot iterations do not
  accumulate versions. Feature creation/refresh happen once, before timing.
- A lightweight `assert` (store materialized, spine non-empty) guards against degenerate empty-state
  timings; exact result values are not checked because they vary per scenario.
