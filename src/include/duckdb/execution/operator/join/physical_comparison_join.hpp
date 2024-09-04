//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"

namespace duckdb {
class ColumnDataCollection;
struct ColumnDataScanState;
class LogicalGet;

//! PhysicalJoin represents the base class of the join operators
class PhysicalComparisonJoin : public PhysicalJoin {
public:
	PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type, vector<JoinCondition> cond,
	                       JoinType join_type, idx_t estimated_cardinality);

	vector<JoinCondition> conditions;
	//! Scans where we should push generated filters into (if any)
	unique_ptr<JoinFilterPushdownInfo> filter_pushdown;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	//! Re-order join conditions so that equality predicates are first, followed by other predicates
	static void ReorderConditions(vector<JoinCondition> &conditions);

	//! Construct the join result of a join with an empty RHS
	static void ConstructEmptyJoinResult(JoinType type, bool has_null, DataChunk &input, DataChunk &result);
	//! Construct the remainder of a Full Outer Join based on which tuples in the RHS found no match
	static void ConstructFullOuterJoinResult(bool *found_match, ColumnDataCollection &input, DataChunk &result,
	                                         ColumnDataScanState &scan_state);
};

} // namespace duckdb
