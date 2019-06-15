//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_nested_loop_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/operator/join/physical_comparison_join.hpp"

namespace duckdb {

index_t nested_loop_join(ExpressionType op, Vector &left, Vector &right, index_t &lpos, index_t &rpos, sel_t lvector[],
                         sel_t rvector[]);
index_t nested_loop_comparison(ExpressionType op, Vector &left, Vector &right, sel_t lvector[], sel_t rvector[],
                               index_t count);

//! PhysicalNestedLoopJoin represents a nested loop join between two tables
class PhysicalNestedLoopJoin : public PhysicalComparisonJoin {
public:
	PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                       vector<JoinCondition> cond, JoinType join_type);

	vector<Expression *> left_expressions;
	vector<Expression *> right_expressions;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalNestedLoopJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalNestedLoopJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), right_chunk(0), has_null(false), left_tuple(0), right_tuple(0) {
		assert(left && right);
	}

	index_t right_chunk;
	DataChunk left_join_condition;
	ChunkCollection right_data;
	ChunkCollection right_chunks;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;

	index_t left_tuple;
	index_t right_tuple;
};
} // namespace duckdb
