//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/physical_nested_loop_join.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

#include "execution/physical_operator.hpp"

#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalNestedLoopJoin represents a nested loop join between two tables
class PhysicalNestedLoopJoin : public PhysicalOperator {
  public:
	PhysicalNestedLoopJoin(std::unique_ptr<PhysicalOperator> left,
	                       std::unique_ptr<PhysicalOperator> right,
	                       std::vector<JoinCondition> cond, JoinType join_type);

	std::vector<TypeId> GetTypes() override;
	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

	std::vector<JoinCondition> conditions;
	JoinType type;
};

class PhysicalNestedLoopJoinOperatorState : public PhysicalOperatorState {
  public:
	PhysicalNestedLoopJoinOperatorState(PhysicalOperator *left,
	                                    PhysicalOperator *right,
	                                    ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), left_position(0),
	      right_chunk(0) {
		assert(left && right);
	}

	size_t left_position;
	size_t right_chunk;
	DataChunk left_join_condition;
	DataChunk right_join_condition;
	ChunkCollection right_chunks;
};
} // namespace duckdb
