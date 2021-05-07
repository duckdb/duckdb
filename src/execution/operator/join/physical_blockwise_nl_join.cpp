#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

namespace duckdb {

PhysicalBlockwiseNLJoin::PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition,
                                                 JoinType join_type, idx_t estimated_cardinality)
    : PhysicalJoin(op, PhysicalOperatorType::BLOCKWISE_NL_JOIN, join_type, estimated_cardinality),
      condition(move(condition)) {
	children.push_back(move(left));
	children.push_back(move(right));
	// MARK, SINGLE and RIGHT OUTER joins not handled
	D_ASSERT(join_type != JoinType::MARK);
	D_ASSERT(join_type != JoinType::SINGLE);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinLocalState : public LocalSinkState {
public:
	BlockwiseNLJoinLocalState() {
	}
};

class BlockwiseNLJoinGlobalState : public GlobalOperatorState {
public:
	BlockwiseNLJoinGlobalState() : right_outer_position(0) {
	}

	ChunkCollection right_chunks;
	//! Whether or not a tuple on the RHS has found a match, only used for FULL OUTER joins
	unique_ptr<bool[]> rhs_found_match;
	//! The position in the RHS in the final scan of the FULL OUTER JOIN
	idx_t right_outer_position;
};

unique_ptr<GlobalOperatorState> PhysicalBlockwiseNLJoin::GetGlobalState(ClientContext &context) {
	return make_unique<BlockwiseNLJoinGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalBlockwiseNLJoin::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<BlockwiseNLJoinLocalState>();
}

void PhysicalBlockwiseNLJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                   DataChunk &input) const {
	auto &gstate = (BlockwiseNLJoinGlobalState &)state;
	gstate.right_chunks.Append(input);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
bool PhysicalBlockwiseNLJoin::Finalize(Pipeline &pipeline, ClientContext &context,
                                       unique_ptr<GlobalOperatorState> state) {
	auto &gstate = (BlockwiseNLJoinGlobalState &)*state;
	if (IsRightOuterJoin(join_type)) {
		gstate.rhs_found_match = unique_ptr<bool[]>(new bool[gstate.right_chunks.Count()]);
		memset(gstate.rhs_found_match.get(), 0, sizeof(bool) * gstate.right_chunks.Count());
	}
	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalBlockwiseNLJoinState : public PhysicalOperatorState {
public:
	PhysicalBlockwiseNLJoinState(PhysicalOperator &op, PhysicalOperator *left, JoinType join_type,
	                             Expression &condition)
	    : PhysicalOperatorState(op, left), left_position(0), right_position(0), fill_in_rhs(false),
	      checked_found_match(false), executor(condition) {
		if (IsLeftOuterJoin(join_type)) {
			lhs_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		}
	}

	//! Whether or not a tuple on the LHS has found a match, only used for LEFT OUTER and FULL OUTER joins
	unique_ptr<bool[]> lhs_found_match;
	idx_t left_position;
	idx_t right_position;
	bool fill_in_rhs;
	bool checked_found_match;
	ExpressionExecutor executor;
};

void PhysicalBlockwiseNLJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalBlockwiseNLJoinState *>(state_p);
	auto &gstate = (BlockwiseNLJoinGlobalState &)*sink_state;

	if (gstate.right_chunks.Count() == 0) {
		// empty RHS
		if (join_type == JoinType::SEMI || join_type == JoinType::INNER) {
			// for SEMI or INNER join: empty RHS means empty result
			return;
		}
		D_ASSERT(join_type == JoinType::LEFT || join_type == JoinType::OUTER || join_type == JoinType::ANTI ||
		         join_type == JoinType::RIGHT);
		// pull a chunk from the LHS
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, true, state->child_chunk, chunk);
		return;
	}

	// now perform the actual join
	// we construct a combined DataChunk by referencing the LHS and the RHS
	// every step that we do not have output results we shift the vectors of the RHS one up or down
	// this creates a new "alignment" between the tuples, exhausting all possible O(n^2) combinations
	// while allowing us to use vectorized execution for every step
	idx_t result_count = 0;
	do {
		if (state->fill_in_rhs) {
			PhysicalComparisonJoin::ConstructFullOuterJoinResult(gstate.rhs_found_match.get(), gstate.right_chunks,
			                                                     chunk, gstate.right_outer_position);
			return;
		}
		if (state->left_position >= state->child_chunk.size()) {
			// exhausted LHS, have to pull new LHS chunk
			if (!state->checked_found_match && state->lhs_found_match) {
				// LEFT OUTER JOIN or FULL OUTER JOIN, first check if we need to create extra results because of
				// non-matching tuples
				SelectionVector sel(STANDARD_VECTOR_SIZE);
				for (idx_t i = 0; i < state->child_chunk.size(); i++) {
					if (!state->lhs_found_match[i]) {
						sel.set_index(result_count++, i);
					}
				}
				if (result_count > 0) {
					// have to create the chunk, set the selection vector and count
					// for the LHS, reference the child_chunk and set the sel_vector and count
					chunk.Slice(state->child_chunk, sel, result_count);
					// for the RHS, set the mask to NULL and set the sel_vector and count
					for (idx_t i = state->child_chunk.ColumnCount(); i < chunk.ColumnCount(); i++) {
						chunk.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
						ConstantVector::SetNull(chunk.data[i], true);
					}
					state->checked_found_match = true;
					return;
				}
			}
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			// no more data on LHS, if FULL OUTER JOIN iterate over RHS
			if (state->child_chunk.size() == 0) {
				if (IsRightOuterJoin(join_type)) {
					state->fill_in_rhs = true;
					continue;
				} else {
					return;
				}
			}
			state->left_position = 0;
			state->right_position = 0;
			if (state->lhs_found_match) {
				state->checked_found_match = false;
				memset(state->lhs_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
		}
		auto &lchunk = state->child_chunk;
		auto &rchunk = gstate.right_chunks.GetChunk(state->right_position);

		// fill in the current element of the LHS into the chunk
		D_ASSERT(chunk.ColumnCount() == lchunk.ColumnCount() + rchunk.ColumnCount());
		for (idx_t i = 0; i < lchunk.ColumnCount(); i++) {
			auto lvalue = lchunk.GetValue(i, state->left_position);
			chunk.data[i].Reference(lvalue);
		}
		// for the RHS we just reference the entire vector
		for (idx_t i = 0; i < rchunk.ColumnCount(); i++) {
			chunk.data[lchunk.ColumnCount() + i].Reference(rchunk.data[i]);
		}
		chunk.SetCardinality(rchunk.size());

		// now perform the computation
		SelectionVector match_sel(STANDARD_VECTOR_SIZE);
		result_count = state->executor.SelectExpression(chunk, match_sel);
		if (result_count > 0) {
			// found a match!
			// set the match flags in the LHS
			if (state->lhs_found_match) {
				state->lhs_found_match[state->left_position] = true;
			}
			// set the match flags in the RHS
			if (gstate.rhs_found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					auto idx = match_sel.get_index(i);
					gstate.rhs_found_match[state->right_position * STANDARD_VECTOR_SIZE + idx] = true;
				}
			}
			chunk.Slice(match_sel, result_count);
		} else {
			// no result: reset the chunk
			chunk.Reset();
		}
		// move to the next tuple on the LHS
		state->left_position++;
		if (state->left_position >= state->child_chunk.size()) {
			// exhausted the current chunk, move to the next RHS chunk
			state->right_position++;
			if (state->right_position < gstate.right_chunks.ChunkCount()) {
				// we still have chunks left! start over on the LHS
				state->left_position = 0;
			}
		}
	} while (result_count == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalBlockwiseNLJoin::GetOperatorState() {
	return make_unique<PhysicalBlockwiseNLJoinState>(*this, children[0].get(), join_type, *condition);
}

string PhysicalBlockwiseNLJoin::ParamsToString() const {
	string extra_info = JoinTypeToString(join_type) + "\n";
	extra_info += condition->GetName();
	return extra_info;
}

} // namespace duckdb
