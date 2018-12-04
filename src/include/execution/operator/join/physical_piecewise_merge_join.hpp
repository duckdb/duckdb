//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

#include "execution/operator/join/physical_join.hpp"

namespace duckdb {

//! PhysicalPiecewiseMergeJoin represents a piecewise merge loop join between
//! two tables
class PhysicalPiecewiseMergeJoin : public PhysicalJoin {
  public:
	struct MergeOrder {
		sel_t order[STANDARD_VECTOR_SIZE];
		size_t count;
	};

	PhysicalPiecewiseMergeJoin(std::unique_ptr<PhysicalOperator> left,
	                           std::unique_ptr<PhysicalOperator> right,
	                           std::vector<JoinCondition> cond,
	                           JoinType join_type);

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<TypeId> join_key_types;
};

class PhysicalPiecewiseMergeJoinOperatorState : public PhysicalOperatorState {
  public:
	PhysicalPiecewiseMergeJoinOperatorState(PhysicalOperator *left,
	                                        PhysicalOperator *right,
	                                        ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), initialized(false),
	      left_position(0), right_position(0), right_chunk_index(0) {
		assert(left && right);
	}

	bool initialized;
	size_t left_position;
	size_t right_position;
	size_t right_chunk_index;
	DataChunk left_chunk;
	DataChunk join_keys;
	PhysicalPiecewiseMergeJoin::MergeOrder left_orders;
	ChunkCollection right_chunks;
	ChunkCollection right_conditions;
	std::vector<PhysicalPiecewiseMergeJoin::MergeOrder> right_orders;
};
} // namespace duckdb
