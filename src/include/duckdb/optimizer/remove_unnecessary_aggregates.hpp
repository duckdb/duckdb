//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_unnecessary_aggregates.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class LogicalAggregate;
class Optimizer;

//! Removes aggregates whose only remaining effect is eliminating duplicate rows that no ancestor can
//! observe, by replacing them with a projection of their groups. This is what makes
//! `SELECT a FROM (SELECT a, b FROM t GROUP BY ALL) GROUP BY a` plan a single aggregation that never reads `b`.
class RemoveUnnecessaryAggregates {
public:
	explicit RemoveUnnecessaryAggregates(Optimizer &optimizer);

	void Optimize(unique_ptr<LogicalOperator> &op);

private:
	//! The operators the not-distinct-dependent property propagated through on the way down. When an aggregate is
	//! removed, these operators suddenly process many more rows, so their cardinality estimates are corrected.
	using OperatorPath = vector<reference<LogicalOperator>>;

	void VisitOperator(unique_ptr<LogicalOperator> &op_ref, AggregateDistinctDependent parent_distinct_dependent,
	                   OperatorPath path);
	bool CanReplaceAggregateWithProjection(const LogicalAggregate &aggr) const;
	void ReplaceAggregateWithProjection(unique_ptr<LogicalOperator> &op_ref, const OperatorPath &path);
	//! Recompute which column bindings are still referenced anywhere in the plan
	void GatherColumnReferences();

private:
	Optimizer &optimizer;
	//! The plan we are optimizing. It is never an aggregate we remove (the query result depends on the duplicate
	//! rows of the topmost operator), so this stays valid for the whole traversal
	optional_ptr<LogicalOperator> plan_root;
	//! Every column binding referenced anywhere in the plan, including the plan's own output
	column_binding_set_t column_references;
};

} // namespace duckdb
