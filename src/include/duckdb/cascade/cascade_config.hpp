//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/cascade/cascade_config.hpp
//
// Runtime switches for the cascade experiment. They exist so that the cascade
// optimizer and DuckDB's own optimizer can be run over the same query and
// compared, both being reachable from one binary.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class CascadeConfig {
public:
	//! DUCKDB_CASCADE=1: hand the bound logical plan to CascadeOptimizer instead
	//! of duckdb::Optimizer.
	static bool UseCascadeOptimizer();

	//! DUCKDB_CASCADE_KEEP_APPLY=1: skip FlattenDependentJoins, so that
	//! LogicalDependentJoin (the Apply operator) survives into the optimizer.
	static bool KeepApply();

	//! DUCKDB_CASCADE_OPTIMIZE=1: after Apply elimination, run DuckDB's own
	//! optimizer over the rewritten plan. Without it the plan goes straight to
	//! the physical planner, which isolates the effect of the rewrite itself.
	static bool RunDuckOptimizers();

	//! DUCKDB_CASCADE_PRINT=0 turns plan printing off (it is on in cascade mode).
	static bool PrintPlans();

	//! DUCKDB_CASCADE_REORDER=0 turns off section 3.1's GroupBy reordering, which is
	//! on by default because the half it implements can only help: the aggregate ends
	//! up seeing fewer rows. It exists so the rewrite's effect can be isolated.
	static bool ReorderGroupBy();

	//! DUCKDB_CASCADE_REORDER_SEMIJOIN=0 keeps the semijoin/antijoin consuming a
	//! GroupBy where it is. Moving it below the GroupBy filters before aggregating,
	//! which is a cost decision the paper leaves to the optimizer - it is on by
	//! default because the loss is small and the gain can be large, but it is the
	//! half that is separable. Only consulted when ReorderGroupBy() is on.
	static bool ReorderSemijoins();

	//! DUCKDB_CASCADE_LOCAL_AGG=1 turns on section 3.3: split an aggregate above an
	//! inner join into a local aggregate below the join and a global one above it.
	//! Off by default - it is a cost decision (the local aggregate only pays off when
	//! it reduces the rows reaching the join), and the paper explicitly leaves it to
	//! the cost-based optimizer.
	static bool PushLocalAggregates();

	//! DUCKDB_CASCADE_AGG_PULLUP=1 turns on section 3.1's GroupBy pull-up: an aggregate
	//! below an inner join moves above it, so that the join gets to reduce the rows the
	//! aggregate reads. It needs the other side of the join to be keyed (the paper's
	//! condition) and it removes the global aggregate, which is why it is a cost decision
	//! and off by default.
	static bool PullUpAggregates();

	//! DUCKDB_CASCADE_AGG_PUSHDOWN=1 turns on section 3.1's GroupBy push-down: an aggregate
	//! above an inner join moves below it and disappears, so the join reads the groups
	//! instead of the rows. It needs the predicate's columns to be grouping columns and the
	//! join to pick at most one row of the other side per group, which is why it is a cost
	//! decision and off by default.
	static bool PushDownAggregates();

	//! DUCKDB_CASCADE_KEYS="part.p_partkey,orders.o_orderkey" makes these columns count as
	//! keys even though the catalog declares no constraint. The TPC-H and TPC-DS generators
	//! emit no primary keys at all (TPC-H has 61 NOT NULL constraints and no UNIQUE, TPC-DS
	//! has none), while the benchmarks' own DDL does declare them - so without this the
	//! key-dependent section 3.1 rules cannot fire on a real warehouse schema and their
	//! coverage cannot be measured. It is an experiment device, and the columns given here
	//! are checked against the data before they are used (see PAPER_AUDIT.md).
	static bool IsDeclaredKey(const string &table_name, const string &column_name);

	//! DUCKDB_CASCADE_FLATTEN_FALLBACK=0 stops shapes outside the cascade rule set
	//! from being handed to DuckDB's own decorrelation (it is on by default).
	static bool FlattenFallback();
};

} // namespace duckdb
