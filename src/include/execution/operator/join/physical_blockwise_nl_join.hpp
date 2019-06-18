//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_blockwise_nl_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/operator/join/physical_join.hpp"

namespace duckdb {

//! PhysicalBlockwiseNLJoin represents a nested loop join between two tables on arbitrary expressions. This is different
//! from the PhysicalNestedLoopJoin in that it does not require expressions to be comparisons between the LHS and the
//! RHS.
class PhysicalBlockwiseNLJoin : public PhysicalJoin {
public:
	PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                        unique_ptr<Expression> condition, JoinType join_type);

	unique_ptr<Expression> condition;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ExtraRenderInformation() const override;
};

class PhysicalBlockwiseNLJoinState : public PhysicalOperatorState {
public:
	PhysicalBlockwiseNLJoinState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), left_position(0), right_position(0), fill_in_rhs(false) {
		assert(left && right);
	}

	//! Whether or not a tuple on the LHS has found a match, only used for LEFT OUTER and FULL OUTER joins
	unique_ptr<bool[]> lhs_found_match;
	//! Whether or not a tuple on the RHS has found a match, only used for FULL OUTER joins
	unique_ptr<bool[]> rhs_found_match;
	ChunkCollection right_chunks;
	index_t left_position;
	index_t right_position;
	bool fill_in_rhs;
	bool checked_found_match;
};
} // namespace duckdb
