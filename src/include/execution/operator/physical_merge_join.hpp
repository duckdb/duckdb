//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_merge_join.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

#include "execution/operator/physical_join.hpp"

namespace duckdb {

//! PhysicalMergeJoin represents a merge loop join between two tables
class PhysicalMergeJoin : public PhysicalJoin {
  public:
	PhysicalMergeJoin(std::unique_ptr<PhysicalOperator> left,
	                  std::unique_ptr<PhysicalOperator> right,
	                  std::vector<JoinCondition> cond, JoinType join_type);

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<TypeId> join_key_types;
};

class PhysicalMergeJoinOperatorState : public PhysicalOperatorState {
  public:
	PhysicalMergeJoinOperatorState(PhysicalOperator *left,
	                               PhysicalOperator *right,
	                               ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), initialized(false),
	      left_position(0), right_position(0), right_chunk_index(0) {
		assert(left && right);
		left_orders = std::unique_ptr<sel_t[]>(new sel_t[STANDARD_VECTOR_SIZE]);
	}

	bool initialized;
	size_t left_position;
	size_t right_position;
	size_t right_chunk_index;
	size_t condition_count;
	DataChunk left_chunk;
	DataChunk join_keys;
	std::unique_ptr<sel_t[]> left_orders;
	ChunkCollection right_chunks;
	std::vector<std::unique_ptr<sel_t[]>> right_orders;
};
} // namespace duckdb
