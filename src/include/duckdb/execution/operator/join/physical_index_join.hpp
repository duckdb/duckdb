//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

//! PhysicalIndexJoin represents an index join between two tables
class PhysicalIndexJoin : public PhysicalOperator {
public:
	PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	                  vector<idx_t> right_projection_map, vector<column_t> column_ids, Index *index, bool lhs_first,
	                  idx_t estimated_cardinality);
	//! Columns from RHS used in the query
	vector<column_t> column_ids;
	//! Columns to be fetched
	vector<column_t> fetch_ids;
	//! Types of fetch columns
	vector<LogicalType> fetch_types;
	//! Columns indexed by index
	unordered_set<column_t> index_ids;
	//! Projected ids from LHS
	vector<column_t> left_projection_map;
	//! Projected ids from RHS
	vector<column_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Index used for join
	Index *index {};

	vector<JoinCondition> conditions;

	JoinType join_type;
	//! In case we swap rhs with lhs we need to output columns related to rhs first.
	bool lhs_first = true;
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) override;

private:
	void GetRHSMatches(ExecutionContext &context, PhysicalOperatorState *state_p) const;
	//! Fills result chunk
	void Output(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p);
};

} // namespace duckdb
