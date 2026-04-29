//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_duplicate_groups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class LogicalAggregate;
class LogicalProjection;
class Optimizer;

//! Removes same-binding duplicate groups (e.g. from Deliminator / RemoveUnusedColumns) and
//! groups that are deterministic functions of a sibling column-ref group (e.g.
//! `GROUP BY x, x-1, cast(x AS BIGINT)`). The latter are recomputed in a projection above.
class RemoveDuplicateGroups : public BaseColumnPruner {
public:
	explicit RemoveDuplicateGroups(Optimizer &optimizer);

	void VisitOperator(LogicalOperator &op) override;

private:
	void VisitAggregate(LogicalAggregate &aggr);

private:
	Optimizer &optimizer;
	//! Stored expressions (kept around so we don't have dangling pointers)
	vector<unique_ptr<Expression>> stored_expressions;
	//! Filled by VisitAggregate, consumed by VisitOperator post-recursion when we own the slot.
	reference_map_t<LogicalAggregate, unique_ptr<LogicalProjection>> pending_projections;
};

} // namespace duckdb
