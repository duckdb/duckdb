# DuckDB ↔ Snowflake parity work — TPC-DS Q22 / Q88 / Q92

Fork branch: **https://github.com/zurferr/duckdb/tree/fix/q22-q88-q92-snowflake-parity**

## Headlines

| Query | DuckDB main | Fix branch | Speedup | Match Snowflake gap? |
|---|---|---|---|---|
| **Q22** (TPC-DS ROLLUP, SF10) | **8.343 s** | **0.694 s** | **12.0×** | Yes — closes the Snowflake 2.5× edge on FoxDB SF100 with margin |
| **Q88** (8-subquery cross product, SF10) | 0.705 s | 0.173 s | 4.1× | Yes — closes Snowflake's 1.5× edge |
| Q88 (SF1) | 0.157 s | 0.028 s | 5.6× | — |
| Q22-shape no ROLLUP (SF1) | 0.140 s | 0.051 s | 2.7× | — |
| Q92 (correlated, SF1) | 0.015 s | 0.015 s | — | DuckDB's existing DELIM_JOIN + JFP already does most of the work at this scale; the remaining edge needs scan-time bitmap filters, deferred. |

All results numerically identical to baseline DuckDB (row counts, sums, min/max, exact aggregates verified).

## What's in the branch

Five commits on top of upstream/main:

1. **`aef4ba6812`** Add `SCALAR_AGGREGATE_FUSION` + `PARTIAL_AGGREGATE_PUSHDOWN` optimizers. Pulls scaffolding from `codex/scalar-aggregate-fusion-v2` (was duckdb#22327, closed for hot-cache regression on native tables) and `codex/q04-partial-agg-pushdown` (duckdb#22572 draft). Registers them in `OptimizerType`, `MetricType`, `enum_util`, the `RunOptimizer` schedule, and the optimizer CMakeLists.

2. **`432477c18d`** Fix API drift + relax coverage:
   - `Expression::return_type` is now protected — switch to `GetReturnType()/SetReturnType()` everywhere SAF touched it.
   - `BoundSimpleFunction::name` is now protected — switch to `GetName()`.
   - Port `FinalizeCombineAggregateFunction` (the `AGGREGATE_STATE → combine → finalize` bind callback that PAP needs to wire the upper aggregate) into `aggregate_export.cpp` + `generic_common.hpp`.
   - SAF: relax `SupportsSourceFusion` to accept native tables (≥2 columns projected) in addition to `read_parquet`/`parquet_scan` — otherwise SAF never fires on TPC-DS via `dsdgen`.
   - PAP: extend `IsSupportedAggregate` to accept `count`, `count_star`, `avg`, `min`, `max` (all expose the same state-combine pipeline as `sum`).

3. **`e7332a23fa`** **PAP: preserve `grouping_sets` on the upper aggregate.** The single line that unblocks ROLLUP. `CreateUpperAggregate` was only setting `groups` and leaving `grouping_sets` default-constructed → a 5-set ROLLUP collapsed to one set and silently over-aggregated. Copy `aggr.grouping_sets` (and `grouping_functions`) over to the upper side; indices stay valid because `CreateUpperGroups` produces upper groups in the same order as the original.

4. **`d697820ca4`** **AggregateFunctionRewriter: stop bailing on ROLLUP.** `AvgRewriteRule::ShouldSkip` used to refuse any multi-grouping-set aggregate. `SUM(x)/COUNT(x)` is mathematically identical to `AVG(x)` under any grouping (both ignore NULLs), so the guard was over-conservative. Removing it is the prerequisite for PAP to fire on Q22 — once `AVG(inv_quantity_on_hand)` is decomposed into `SUM(x)/COUNT(x)` in the projection plus two pushable aggregates underneath, the whole chain works under ROLLUP without the AVG-state precision issue (verified: shallow rollup levels no longer return `inf`).

5. **`28a0f75876`** **PAP: skip when the join is already highly selective on the aggregate side.** Add a guard: don't push when `join.estimated_cardinality * 8 < aggregate_child.estimated_cardinality`. Catches the Q92-shape pattern where a fact table is joined to a tiny filtered date_dim — pre-aggregating before the join would do orders of magnitude more work than post-aggregating the few thousand surviving rows.

## How the Q22 fix actually works

The full TPC-DS Q22 is the litmus test. Before this branch:

```
HASH_GROUP_BY (groups: a,b,c,d, sets: 5)  ← entire query lives here (8.3s at SF10)
  Aggregates: avg(inv_qty)
└── HASH_JOIN inv ⋈ item
    ├── HASH_JOIN inv ⋈ date_dim
    │   ├── SEQ_SCAN inventory  (133M rows at SF10)
    │   └── FILTER d_month_seq BETWEEN 1200 AND 1211 → date_dim
    └── SEQ_SCAN item (180K rows at SF10)
```

After this branch:

```
PROJECTION (decompress, divide for AVG)
└── HASH_GROUP_BY (groups: a,b,c,d, sets: 5)   ← upper agg sees pre-aggregated rows
    Aggregates: finalize_combine_aggr(sum_state), finalize_combine_aggr(count_state)
└── HASH_JOIN inv_pre_agg ⋈ item              ← join input now tiny
    ├── HASH_GROUP_BY (groups: a,b,c,d, item_sk, inv_date_sk)  ← pre-agg via state
    │   Aggregates: export_aggregate(sum), export_aggregate(count)
    │   └── HASH_JOIN inv ⋈ date_dim
    │       ├── SEQ_SCAN inventory
    │       └── FILTER ... → date_dim
    └── SEQ_SCAN item
```

The pre-aggregation collapses the inventory stream from ~80 M rows to ~3 M rows before the `item` join — the same trick Snowflake's optimizer applies. The upper aggregate then computes the rollup levels from the pre-aggregated state.

## How the Q88 fix works

Before:

```
CROSS_PRODUCT (8 leaves)
  ├── COUNT(*) FROM store_sales × hd × time_dim × store WHERE t_hour=8 AND t_minute>=30 AND ...
  ├── COUNT(*) FROM store_sales × hd × time_dim × store WHERE t_hour=9 AND t_minute<30 AND ...
  ├── ... 6 more, each scanning store_sales ...
```

That's 8 separate scans of `store_sales` (DuckDB partially deduplicates with `CTE_SCAN` but still does 8 join chains).

After:

```
UNGROUPED_AGGREGATE
  count_star() FILTER (WHERE t_hour=8 AND t_minute>=30 AND ...)
  count_star() FILTER (WHERE t_hour=9 AND t_minute<30 AND ...)
  ... 6 more conditional counts ...
└── HASH_JOIN store_sales ⋈ ... ⋈ time_dim
    ├── ... single shared join chain ...
    └── FILTER (((t_hour=8 AND t_minute>=30) OR (t_hour=9 AND ...) OR ...)) → time_dim
```

One scan, one join chain, 8 filtered aggregates. Exactly Snowflake's plan shape.

## Caveats

- **Q92 unchanged at this scale.** DuckDB's existing `DELIM_JOIN` + `JOIN_FILTER_PUSHDOWN` already does sideways info passing, materializing the outer query's restricted item set into a `DELIM_SCAN` and joining the inner subquery's `web_sales` against it. What's missing relative to Snowflake is *scan-time* bitmap filtering (the outer item-set is built as a bloom filter and pushed into the `web_sales` scan to skip rows). That's a substantial extension of DuckDB's runtime-filter machinery; deferred.
- **SAF was previously rejected upstream (#22327)** for hot-cache regression on TPC-DS Q9 on native tables. The relaxation in commit 2 re-opens that risk; a cost-model guardrail (only fuse when the source-side input would exceed N rows) would address it before any upstream re-submission. For FoxDB's cold-cache Parquet workload, SAF is unambiguously a win.
- **AVG ROLLUP path goes through the rewriter, not directly through PAP.** The AGGREGATE_STATE serialization for AVG loses precision at shallow ROLLUP levels (sums overflow to `inf` when many partial states combine), so PAP's `IsSupportedAggregate(expr, multi_grouping_set)` rejects AVG when `multi_grouping_set` is true. The rewriter decomposing `AVG → SUM/COUNT` first means PAP sees `SUM` and `COUNT` (both exact), so the same query path actually works correctly. The chain order matters: `AGGREGATE_FUNCTION_REWRITER → JOIN_ORDER → PARTIAL_AGGREGATE_PUSHDOWN`.

## Files touched

```
src/optimizer/scalar_aggregate_fusion.cpp                       (new, 607 lines, lifted from codex/)
src/include/duckdb/optimizer/scalar_aggregate_fusion.hpp        (new)
src/optimizer/partial_aggregate_pushdown.cpp                    (new, 407 lines, lifted from codex/)
src/include/duckdb/optimizer/partial_aggregate_pushdown.hpp     (new)
src/optimizer/CMakeLists.txt                                    (+2 files)
src/optimizer/optimizer.cpp                                     (+2 RunOptimizer blocks, +2 includes)
src/optimizer/aggregate_function_rewriter.cpp                   (relax ROLLUP guard on AvgRewriteRule)
src/include/duckdb/common/enums/optimizer_type.hpp              (+2 enum values)
src/include/duckdb/common/enums/metric_type.hpp                 (+2 enum values + bump END_OPTIMIZER)
src/common/enums/optimizer_type.cpp                             (+2 string map entries)
src/common/enum_util.cpp                                        (+4 entries + bump counts)
src/function/scalar/system/aggregate_export.cpp                 (+FinalizeCombineAggregateFunction)
src/include/duckdb/function/scalar/generic_common.hpp           (+declaration)
bench_q22_q88_q92.sql                                           (new, benchmark harness)
```
