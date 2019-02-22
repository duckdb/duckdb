//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/merge_join.hpp"
#include "execution/operator/join/physical_join.hpp"

namespace duckdb {

//! PhysicalPiecewiseMergeJoin represents a piecewise merge loop join between
//! two tables
class PhysicalPiecewiseMergeJoin : public PhysicalJoin {
public:
	PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                           unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type);

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *parent_executor) override;

	vector<TypeId> join_key_types;
};

class PhysicalPiecewiseMergeJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalPiecewiseMergeJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right,
	                                        ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), initialized(false), left_position(0), right_position(0),
	      right_chunk_index(0), has_null(false) {
		assert(left && right);
	}

	bool initialized;
	size_t left_position;
	size_t right_position;
	size_t right_chunk_index;
	DataChunk left_chunk;
	DataChunk join_keys;
	MergeOrder left_orders;
	ChunkCollection right_chunks;
	ChunkCollection right_conditions;
	vector<MergeOrder> right_orders;
	bool has_null;
};
} // namespace duckdb
