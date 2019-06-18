#include "execution/operator/join/physical_blockwise_nl_join.hpp"

#include "common/types/static_vector.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

PhysicalBlockwiseNLJoin::PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition,
                                                 JoinType join_type)
    : PhysicalJoin(op, PhysicalOperatorType::BLOCKWISE_NL_JOIN, join_type), condition(move(condition)) {
	children.push_back(move(left));
	children.push_back(move(right));
	// MARK, SINGLE and RIGHT OUTER joins not handled
	assert(join_type != JoinType::MARK);
	assert(join_type != JoinType::RIGHT);
	assert(join_type != JoinType::SINGLE);
}

void PhysicalBlockwiseNLJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalBlockwiseNLJoinState *>(state_);

	// first we fully materialize the right child, if we haven't done that yet
	if (state->right_chunks.column_count() == 0) {
		auto right_state = children[1]->GetOperatorState();
		auto left_types = children[0]->GetTypes();
		auto right_types = children[1]->GetTypes();

		DataChunk right_chunk;
		right_chunk.Initialize(right_types);
		while (true) {
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			state->right_chunks.Append(right_chunk);
		}

		if (state->right_chunks.count == 0) {
			if ((type == JoinType::INNER || type == JoinType::SEMI)) {
				// empty RHS with INNER or SEMI join means empty result set
				return;
			}
		}
		// initialize the found_match vectors for the left and right sides
		if (type == JoinType::LEFT || type == JoinType::OUTER) {
			state->lhs_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
		}
		if (type == JoinType::OUTER) {
			state->rhs_found_match = unique_ptr<bool[]>(new bool[state->right_chunks.count]);
			memset(state->rhs_found_match.get(), 0, sizeof(bool) * state->right_chunks.count);
		}
	}

	if (state->right_chunks.count == 0) {
		// empty join
		assert(type == JoinType::LEFT || type == JoinType::OUTER || type == JoinType::ANTI);
		// pull a chunk from the LHS
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// fill in the data from the chunk
		for (index_t i = 0; i < state->child_chunk.column_count; i++) {
			chunk.data[i].Reference(state->child_chunk.data[i]);
		}
		if (type == JoinType::LEFT || type == JoinType::OUTER) {
			// LEFT OUTER or FULL OUTER join with empty RHS
			// fill any columns from the RHS with NULLs
			chunk.sel_vector = chunk.data[0].sel_vector;
			for (index_t i = state->child_chunk.column_count; i < chunk.column_count; i++) {
				chunk.data[i].count = chunk.size();
				chunk.data[i].sel_vector = chunk.sel_vector;
				VectorOperations::Set(chunk.data[i], Value());
			}
		}
		return;
	}

	// now perform the actual join
	// we construct a combined DataChunk by referencing the LHS and the RHS
	// every step that we do not have output results we shift the vectors of the RHS one up or down
	// this creates a new "alignment" between the tuples, exhausting all possible O(n^2) combinations
	// while allowing us to use vectorized execution for every step
	StaticVector<bool> result;
	index_t result_count = 0;
	do {
		if (state->fill_in_rhs) {
			throw NotImplementedException("FIXME: full outer join");
		}
		if (state->left_position >= state->child_chunk.size()) {
			// exhausted LHS, have to pull new LHS chunk
			if (!state->checked_found_match && state->lhs_found_match) {
				// LEFT OUTER JOIN or FULL OUTER JOIN, first check if we need to create extra results because of
				// non-matching tuples
				for (index_t i = 0; i < state->child_chunk.size(); i++) {
					if (!state->lhs_found_match[i]) {
						chunk.owned_sel_vector[result_count++] = i;
					}
				}
				if (result_count > 0) {
					// have to create the chunk, set the selection vector and count
					chunk.sel_vector = chunk.owned_sel_vector;
					// for the LHS, reference the child_chunk and set the sel_vector and count
					for (index_t i = 0; i < state->child_chunk.column_count; i++) {
						chunk.data[i].Reference(state->child_chunk.data[i]);
						chunk.data[i].sel_vector = chunk.sel_vector;
						chunk.data[i].count = result_count;
					}
					// for the RHS, set the mask to NULL and set the sel_vector and count
					for (index_t i = state->child_chunk.column_count; i < chunk.column_count; i++) {
						chunk.data[i].nullmask.set();
						chunk.data[i].sel_vector = chunk.sel_vector;
						chunk.data[i].count = result_count;
					}
					state->checked_found_match = true;
					return;
				}
			}
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			// no more data on LHS, if FULL OUTER JOIN iterate over RHS
			if (state->child_chunk.size() == 0) {
				if (type == JoinType::OUTER) {
					state->fill_in_rhs = true;
					continue;
				} else {
					return;
				}
			}
			state->child_chunk.Flatten();
			state->left_position = 0;
			state->right_position = 0;
			if (state->lhs_found_match) {
				state->checked_found_match = false;
				memset(state->lhs_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
		}
		auto &lchunk = state->child_chunk;
		auto &rchunk = *state->right_chunks.chunks[state->right_position];
		for (index_t i = 0; i < chunk.column_count; i++) {
			assert(!chunk.data[i].sel_vector);
			chunk.data[i].count = rchunk.size();
		}
		// fill in the current element of the LHS into the chunk
		assert(chunk.column_count == lchunk.column_count + rchunk.column_count);
		for (index_t i = 0; i < lchunk.column_count; i++) {
			VectorOperations::Set(chunk.data[i], lchunk.data[i].GetValue(state->left_position));
		}
		// for the RHS we just reference the entire vector
		for (index_t i = 0; i < rchunk.column_count; i++) {
			chunk.data[lchunk.column_count + i].Reference(rchunk.data[i]);
		}
		// now perform the computation
		ExpressionExecutor executor(chunk);
		executor.ExecuteExpression(*condition, result);

		// create the result
		VectorOperations::ExecType<bool>(result, [&](bool match, index_t i, index_t k) {
			if (match && !result.nullmask[i]) {
				// found a match!
				// set the match flags
				if (state->lhs_found_match) {
					state->lhs_found_match[state->left_position] = true;
					if (state->rhs_found_match) {
						state->rhs_found_match[state->right_position * STANDARD_VECTOR_SIZE + i] = true;
					}
				}
				chunk.owned_sel_vector[result_count++] = i;
			}
		});

		if (result_count > 0) {
			// had a result! set the selection vector of the child elements
			chunk.sel_vector = chunk.owned_sel_vector;
			for (index_t i = 0; i < chunk.column_count; i++) {
				chunk.data[i].sel_vector = chunk.sel_vector;
				chunk.data[i].count = result_count;
			}
		} else {
			// no result: reset the chunk
			chunk.Reset();
		}
		// move to the next tuple on the LHS
		state->left_position++;
		if (state->left_position >= state->child_chunk.size()) {
			// exhausted the current chunk, move to the next RHS chunk
			state->right_position++;
			if (state->right_position < state->right_chunks.chunks.size()) {
				// we still have chunks left! start over on the LHS
				state->left_position = 0;
			}
		}
	} while (result_count == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalBlockwiseNLJoin::GetOperatorState() {
	return make_unique<PhysicalBlockwiseNLJoinState>(children[0].get(), children[1].get());
}

string PhysicalBlockwiseNLJoin::ExtraRenderInformation() const {
	string extra_info = JoinTypeToString(type) + "\n";
	extra_info += condition->GetName();
	return extra_info;
}
