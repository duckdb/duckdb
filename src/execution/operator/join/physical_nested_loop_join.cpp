#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/nested_loop_join.hpp"

using namespace std;

namespace duckdb {

class PhysicalNestedLoopJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalNestedLoopJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), right_chunk(0), has_null(false), left_tuple(0),
	      right_tuple(0) {
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

PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                               JoinType join_type)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond), join_type) {
	children.push_back(move(left));
	children.push_back(move(right));
}

//! Remove NULL values from a chunk; returns true if the chunk had NULL values
static bool RemoveNullValues(DataChunk &chunk) {
	// OR all nullmasks together
	nullmask_t nullmask = chunk.data[0].nullmask;
	for (index_t i = 1; i < chunk.column_count(); i++) {
		nullmask |= chunk.data[i].nullmask;
	}
	// now create a selection vector
	sel_t not_null_vector[STANDARD_VECTOR_SIZE];
	index_t not_null_entries = 0;
	VectorOperations::Exec(chunk.data[0], [&](index_t i, index_t k) {
		if (!nullmask[i]) {
			not_null_vector[not_null_entries++] = i;
		}
	});
	assert(not_null_entries <= chunk.size());
	if (not_null_entries < chunk.size()) {
		// found NULL entries!
		assert(sizeof(not_null_vector) == sizeof(chunk.owned_sel_vector));
		memcpy(chunk.owned_sel_vector, not_null_vector, sizeof(not_null_vector));

		chunk.SetCardinality(not_null_entries, chunk.owned_sel_vector);
		chunk.Verify();
		return true;
	} else {
		return false;
	}
}

template <bool MATCH>
static void ConstructSemiOrAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	assert(left.column_count() == result.column_count());
	// create the selection vector from the matches that were found
	index_t result_count = 0;
	for (index_t i = 0; i < left.size(); i++) {
		if (found_match[i] == MATCH) {
			// part of the result
			result.owned_sel_vector[result_count++] = i;
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// project them using the result selection vector
		// reference the columns of the left side from the result
		for (index_t i = 0; i < left.column_count(); i++) {
			result.data[i].Reference(left.data[i]);
		}
		result.SetCardinality(result_count, result.owned_sel_vector);
	} else {
		result.SetCardinality(0);
	}
}

void PhysicalNestedLoopJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalNestedLoopJoinState *>(state_);

	// first we fully materialize the right child, if we haven't done that yet
	if (state->right_chunks.column_count() == 0) {
		vector<TypeId> condition_types;
		for (auto &cond : conditions) {
			assert(cond.left->return_type == cond.right->return_type);
			condition_types.push_back(cond.left->return_type);
		}

		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();

		DataChunk new_chunk, right_condition;
		new_chunk.Initialize(types);
		right_condition.Initialize(condition_types);
		do {
			children[1]->GetChunk(context, new_chunk, right_state.get());
			if (new_chunk.size() == 0) {
				break;
			}
			// resolve the join expression of the right side
			state->rhs_executor.Execute(new_chunk, right_condition);

			state->right_data.Append(new_chunk);
			state->right_chunks.Append(right_condition);
		} while (new_chunk.size() > 0);

		if (state->right_chunks.count == 0) {
			if ((type == JoinType::INNER || type == JoinType::SEMI)) {
				// empty RHS with INNER or SEMI join means empty result set
				return;
			}
		} else {
			// disqualify tuples from the RHS that have NULL values
			for (index_t i = 0; i < state->right_chunks.chunks.size(); i++) {
				if (RemoveNullValues(*state->right_chunks.chunks[i])) {
					state->has_null = true;
				}
			}
			// initialize the chunks for the join conditions
			state->left_join_condition.Initialize(condition_types);
			state->right_chunk = state->right_chunks.chunks.size() - 1;
			state->right_tuple = state->right_chunks.chunks[state->right_chunk]->size();
		}
	}

	if (state->right_chunks.count == 0) {
		// empty join, switch on type
		if (type == JoinType::MARK) {
			// pull a chunk from the LHS
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			// RHS empty: just set found_match to false
			bool found_match[STANDARD_VECTOR_SIZE] = {false};
			ConstructMarkJoinResult(state->left_join_condition, state->child_chunk, chunk, found_match,
			                        state->has_null);
		} else if (type == JoinType::ANTI) {
			// ANTI join, just pull chunk from RHS
			children[0]->GetChunk(context, chunk, state->child_state.get());
		} else if (type == JoinType::LEFT) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			chunk.SetCardinality(state->child_chunk);
			index_t idx = 0;
			for (; idx < state->child_chunk.column_count(); idx++) {
				chunk.data[idx].Reference(state->child_chunk.data[idx]);
			}
			for (; idx < chunk.column_count(); idx++) {
				chunk.data[idx].vector_type = VectorType::CONSTANT_VECTOR;
				chunk.data[idx].nullmask[0] = true;
			}
		} else {
			throw Exception("Unhandled type for empty NL join");
		}
		return;
	}

	if ((type == JoinType::INNER || type == JoinType::LEFT) &&
	    state->right_chunk >= state->right_chunks.chunks.size()) {
		return;
	}
	// now that we have fully materialized the right child
	// we have to perform the nested loop join
	do {
		// first check if we have to move to the next child on the right isde
		assert(state->right_chunk < state->right_chunks.chunks.size());
		if (state->right_tuple >= state->right_chunks.chunks[state->right_chunk]->size()) {
			// we exhausted the chunk on the right
			state->right_chunk++;
			if (state->right_chunk >= state->right_chunks.chunks.size()) {
				// we exhausted all right chunks!
				// move to the next left chunk
				do {
					children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
					if (state->child_chunk.size() == 0) {
						return;
					}
					state->child_chunk.ClearSelectionVector();

					// resolve the left join condition for the current chunk
					state->lhs_executor.Execute(state->child_chunk, state->left_join_condition);
					if (type != JoinType::MARK) {
						// immediately disqualify any tuples from the left side that have NULL values
						// we don't do this for the MARK join on the LHS, because the tuple will still be output, just
						// with a NULL marker!
						RemoveNullValues(state->left_join_condition);
					}
				} while (state->left_join_condition.size() == 0);

				state->right_chunk = 0;
			}
			// move to the start of this chunk
			state->left_tuple = 0;
			state->right_tuple = 0;
		}

		switch (type) {
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK: {
			// MARK, SEMI and ANTI joins are handled separately because they scan the whole RHS in one go
			bool found_match[STANDARD_VECTOR_SIZE] = {false};
			NestedLoopJoinMark::Perform(state->left_join_condition, state->right_chunks, found_match, conditions);
			if (type == JoinType::MARK) {
				// now construct the mark join result from the found matches
				ConstructMarkJoinResult(state->left_join_condition, state->child_chunk, chunk, found_match,
				                        state->has_null);
			} else if (type == JoinType::SEMI) {
				// construct the semi join result from the found matches
				ConstructSemiOrAntiJoinResult<true>(state->child_chunk, chunk, found_match);
			} else if (type == JoinType::ANTI) {
				ConstructSemiOrAntiJoinResult<false>(state->child_chunk, chunk, found_match);
			}
			// move to the next LHS chunk in the next iteration
			state->right_tuple = state->right_chunks.chunks[state->right_chunk]->size();
			state->right_chunk = state->right_chunks.chunks.size() - 1;
			return;
		}
		default:
			break;
		}

		auto &left_chunk = state->child_chunk;
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk];
		auto &right_data = *state->right_data.chunks[state->right_chunk];

		// sanity check
		left_chunk.Verify();
		right_chunk.Verify();
		right_data.Verify();

		// now perform the join
		switch (type) {
		case JoinType::INNER: {
			sel_t lvector[STANDARD_VECTOR_SIZE], rvector[STANDARD_VECTOR_SIZE];
			index_t match_count =
			    NestedLoopJoinInner::Perform(state->left_tuple, state->right_tuple, state->left_join_condition,
			                                 right_chunk, lvector, rvector, conditions);
			// we have finished resolving the join conditions
			if (match_count == 0) {
				// if there are no results, move on
				continue;
			}
			// we have matching tuples!
			// construct the result
			// create a reference to the chunk on the left side using the lvector
			// VectorCardinality lcardinality(match_count, lvector);
			chunk.SetCardinality(match_count, lvector);
			for (index_t i = 0; i < state->child_chunk.column_count(); i++) {
				chunk.data[i].Reference(state->child_chunk.data[i]);
				chunk.data[i].ClearSelectionVector();
			}
			chunk.SetCardinality(match_count, rvector);
			// now create a reference to the chunk on the right side using the rvector
			// VectorCardinality rcardinality(match_count, rvector);
			for (index_t i = 0; i < right_data.column_count(); i++) {
				index_t chunk_entry = state->child_chunk.column_count() + i;
				chunk.data[chunk_entry].Reference(right_data.data[i]);
				chunk.data[chunk_entry].ClearSelectionVector();
			}
			chunk.SetCardinality(match_count);
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for nested loop join!");
		}
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalNestedLoopJoin::GetOperatorState() {
	return make_unique<PhysicalNestedLoopJoinState>(children[0].get(), children[1].get(), conditions);
}

} // namespace duckdb
