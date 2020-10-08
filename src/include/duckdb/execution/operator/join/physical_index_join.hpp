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

namespace duckdb {

//! PhysicalIndexJoin represents an index join between two tables
class PhysicalIndexJoin : public PhysicalOperator {
public:
	PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type, const vector<idx_t>& left_projection_map,
	                  vector<idx_t> right_projection_map,vector<column_t> column_ids, Index *index, bool lhs_first);
	//! Columns from RHS used in the query
	vector<column_t> column_ids;
	//! Projected ids from LHS
	vector<idx_t> left_projection_map;
	//! Projected ids from RHS
	vector<idx_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Index used for join
	Index* index {};

    vector<JoinCondition> conditions;

    JoinType join_type;
    //! In case we swap rhs with lhs we need to output columns related to rhs first.
	bool lhs_first = true;
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
