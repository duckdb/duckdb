# Recursive CTE research benchmarks

The SQL workloads in this directory are adapted from the queries accompanying
[How DuckDB is `USING KEY` to Unlock Recursive Query Performance](https://db.cs.uni-tuebingen.de/publications/2025/using-key/).
They cover breadth-first search, Bellman-Ford, connected components, distance
vector routing, Conway's Game of Life, k-means, PageRank, and Kruskal's
algorithm.

The adaptations make every query deterministic and self-contained, use the
current `USING KEY ... UNION ALL` syntax, and return compact results suitable
for regression testing. The original algorithms and recurring-table access
patterns are retained. The queries are available under the MIT license in
this directory.

Build and run the interpreted benchmarks with:

```shell
BUILD_BENCHMARK=1 make release
build/release/benchmark/benchmark_runner "benchmark/recursive_cte/.*"
```

The `smoke` queries retain the small demonstration inputs. The `performance`
queries scale representative access patterns and are not part of the regular
unit-test suite.

The optional `queries/ldbc/dvr.sql` workload runs the published DVR access
pattern over DuckDB's existing LDBC SF0.1 dataset. Follow the setup steps in
`benchmark/ldbc/README.md`, then run:

```shell
build/release/duckdb ldbc.duckdb -f benchmark/recursive_cte/queries/ldbc/dvr.sql
```

The downloaded LDBC data and generated database are not part of this suite.
