//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/physical_nested_loop_join.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"

#include "execution/operator/physical_join.hpp"

namespace duckdb {

//! PhysicalNestedLoopJoin represents a nested loop join between two tables
class PhysicalNestedLoopJoin : public PhysicalJoin {
  public:
	PhysicalNestedLoopJoin(std::unique_ptr<PhysicalOperator> left,
	                       std::unique_ptr<PhysicalOperator> right,
	                       std::vector<JoinCondition> cond, JoinType join_type);

	virtual void _GetChunk(ClientContext &context, DataChunk &chunk,
	                       PhysicalOperatorState *state) override;

	virtual std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent_executor) override;

  private:
	// method to create the final result given the vector of matches of the left
	// tuple with the right chunk
	bool CreateResult(DataChunk &left, size_t left_position, DataChunk &right,
	                  DataChunk &result, Vector &matches, bool is_last_chunk);
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
