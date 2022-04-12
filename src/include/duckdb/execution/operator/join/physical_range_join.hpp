//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

struct GlobalSortState;

//! PhysicalRangeJoin represents one or more inequality range join predicates between
//! two tables
class PhysicalRangeJoin : public PhysicalComparisonJoin {
public:
	PhysicalRangeJoin(LogicalOperator &op, PhysicalOperatorType type, unique_ptr<PhysicalOperator> left,
	                  unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
	                  idx_t estimated_cardinality);

public:
	// Merge the NULLs of all non-DISTINCT predicates into the primary so they sort to the end.
	idx_t MergeNulls(DataChunk &keys) const;
	// Gather the result values and slice the payload columns to those values.
	static void SliceSortedPayload(DataChunk &payload, GlobalSortState &state, const idx_t block_idx,
	                               const SelectionVector &result, const idx_t result_count, const idx_t left_cols = 0);
	// Apply a tail condition to the current selection
	static idx_t SelectJoinTail(const ExpressionType &condition, Vector &left, Vector &right,
	                            const SelectionVector *sel, idx_t count, SelectionVector *true_sel);
};

} // namespace duckdb
