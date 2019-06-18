//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_piecewise_merge_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/merge_join.hpp"
#include "execution/operator/join/physical_comparison_join.hpp"

namespace duckdb {

//! PhysicalPiecewiseMergeJoin represents a piecewise merge loop join between
//! two tables
class PhysicalPiecewiseMergeJoin : public PhysicalComparisonJoin {
public:
	PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                           unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type);

	vector<TypeId> join_key_types;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalPiecewiseMergeJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalPiecewiseMergeJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), initialized(false), left_position(0), right_position(0), right_chunk_index(0),
	      has_null(false) {
		assert(left && right);
	}

	bool initialized;
	index_t left_position;
	index_t right_position;
	index_t right_chunk_index;
	DataChunk left_chunk;
	DataChunk join_keys;
	MergeOrder left_orders;
	ChunkCollection right_chunks;
	ChunkCollection right_conditions;
	vector<MergeOrder> right_orders;
	bool has_null;
};
} // namespace duckdb
