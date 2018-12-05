//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_nested_loop_join_inner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/operator/join/physical_join.hpp"

namespace duckdb {

size_t nested_loop_join(ExpressionType op, Vector &left, Vector &right, size_t &lpos, size_t &rpos, sel_t lvector[],
                        sel_t rvector[]);
size_t nested_loop_comparison(ExpressionType op, Vector &left, Vector &right, sel_t lvector[], sel_t rvector[],
                              size_t count);

//! PhysicalNestedLoopJoinInner represents an inner nested loop join between two
//! tables
class PhysicalNestedLoopJoinInner : public PhysicalJoin {
public:
	PhysicalNestedLoopJoinInner(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                            unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type);

	void _GetChunk(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState(ExpressionExecutor *parent_executor) override;
};

class PhysicalNestedLoopJoinInnerOperatorState : public PhysicalOperatorState {
public:
	PhysicalNestedLoopJoinInnerOperatorState(PhysicalOperator *left, PhysicalOperator *right,
	                                         ExpressionExecutor *parent_executor)
	    : PhysicalOperatorState(left, parent_executor), right_chunk(0), left_tuple(0), right_tuple(0) {
		assert(left && right);
	}

	size_t right_chunk;
	DataChunk left_join_condition;
	ChunkCollection right_data;
	ChunkCollection right_chunks;

	size_t left_tuple;
	size_t right_tuple;
};
} // namespace duckdb
