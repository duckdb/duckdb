//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/operator/join/physical_join.hpp"

namespace duckdb {

//! PhysicalMergeJoin represents a merge loop join between two tables
class PhysicalMergeJoin : public PhysicalJoin {
public:
	PhysicalMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                  vector<JoinCondition> cond, JoinType join_type);

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *parent_executor) override;

	vector<TypeId> join_key_types;
};

class PhysicalMergeJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalMergeJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right, ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), initialized(false), left_position(0), right_position(0),
	      right_chunk_index(0) {
		assert(left && right);
		left_orders = unique_ptr<sel_t[]>(new sel_t[STANDARD_VECTOR_SIZE]);
	}

	bool initialized;
	size_t left_position;
	size_t right_position;
	size_t right_chunk_index;
	size_t condition_count;
	DataChunk left_chunk;
	DataChunk join_keys;
	unique_ptr<sel_t[]> left_orders;
	ChunkCollection right_chunks;
	vector<unique_ptr<sel_t[]>> right_orders;
};
} // namespace duckdb
