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

	idx_t right_chunk;
	DataChunk left_join_condition;
	ChunkCollection right_data;
	ChunkCollection right_chunks;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;

	idx_t left_tuple;
	idx_t right_tuple;

	unique_ptr<bool[]> left_found_match;
};

PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                               JoinType join_type)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond), join_type) {
	children.push_back(move(left));
	children.push_back(move(right));
}

static bool HasNullValues(DataChunk &chunk) {
	for (idx_t col_idx = 0; col_idx < chunk.column_count(); col_idx++) {
		VectorData vdata;
		chunk.data[col_idx].Orrify(chunk.size(), vdata);

		if (vdata.nullmask->none()) {
			continue;
		}
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = vdata.sel->get_index(i);
			if ((*vdata.nullmask)[idx]) {
				return true;
			}
		}
	}
	return false;
}

template <bool MATCH>
void PhysicalJoin::ConstructSemiOrAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	assert(left.column_count() == result.column_count());
	// create the selection vector from the matches that were found
	idx_t result_count = 0;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < left.size(); i++) {
		if (found_match[i] == MATCH) {
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// project them using the result selection vector
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		result.SetCardinality(0);
	}
}

void PhysicalJoin::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
                                           bool has_null) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(left);
	for (idx_t i = 0; i < left.column_count(); i++) {
		result.data[i].Reference(left.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.vector_type = VectorType::FLAT_VECTOR;
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &nullmask = FlatVector::Nullmask(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.column_count(); col_idx++) {
		VectorData jdata;
		join_keys.data[col_idx].Orrify(join_keys.size(), jdata);
		if (jdata.nullmask->any()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				nullmask[i] = (*jdata.nullmask)[jidx];
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < left.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * left.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (has_null) {
		for (idx_t i = 0; i < left.size(); i++) {
			if (!bool_result[i]) {
				nullmask[i] = true;
			}
		}
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
			// for the MARK join, we check if there are null values in any of the right chunks
			if (type == JoinType::MARK) {
				for (idx_t i = 0; i < state->right_chunks.chunks.size(); i++) {
					if (HasNullValues(*state->right_chunks.chunks[i])) {
						state->has_null = true;
					}
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
			// RHS empty: set FOUND MATCh vector to false
			chunk.Reference(state->child_chunk);
			auto &mark_vector = chunk.data.back();
			mark_vector.vector_type = VectorType::CONSTANT_VECTOR;
			mark_vector.SetValue(0, Value::BOOLEAN(false));
		} else if (type == JoinType::ANTI) {
			// ANTI join, just pull chunk from RHS
			children[0]->GetChunk(context, chunk, state->child_state.get());
		} else if (type == JoinType::LEFT) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			chunk.Reference(state->child_chunk);
			for (idx_t idx = state->child_chunk.column_count(); idx < chunk.column_count(); idx++) {
				chunk.data[idx].vector_type = VectorType::CONSTANT_VECTOR;
				ConstantVector::SetNull(chunk.data[idx], true);
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
					if (type == JoinType::LEFT) {
						// left join: before we move to the next chunk, see if we need to output any vectors that didn't
						// have a match found
						if (state->left_found_match) {
							SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
							idx_t remaining_count = 0;
							for (idx_t i = 0; i < state->child_chunk.size(); i++) {
								if (!state->left_found_match[i]) {
									remaining_sel.set_index(remaining_count++, i);
								}
							}
							state->left_found_match.reset();
							chunk.Slice(state->child_chunk, remaining_sel, remaining_count);
							for (idx_t idx = state->child_chunk.column_count(); idx < chunk.column_count(); idx++) {
								chunk.data[idx].vector_type = VectorType::CONSTANT_VECTOR;
								ConstantVector::SetNull(chunk.data[idx], true);
							}
						} else {
							state->left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
							memset(state->left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
						}
					}
					children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
					if (state->child_chunk.size() == 0) {
						return;
					}

					// resolve the left join condition for the current chunk
					state->lhs_executor.Execute(state->child_chunk, state->left_join_condition);
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
				PhysicalJoin::ConstructMarkJoinResult(state->left_join_condition, state->child_chunk, chunk,
				                                      found_match, state->has_null);
			} else if (type == JoinType::SEMI) {
				// construct the semi join result from the found matches
				PhysicalJoin::ConstructSemiOrAntiJoinResult<true>(state->child_chunk, chunk, found_match);
			} else if (type == JoinType::ANTI) {
				PhysicalJoin::ConstructSemiOrAntiJoinResult<false>(state->child_chunk, chunk, found_match);
			}
			// move to the next LHS chunk in the next iteration
			state->right_tuple = state->right_chunks.chunks[state->right_chunk]->size();
			state->right_chunk = state->right_chunks.chunks.size() - 1;
			if (chunk.size() > 0) {
				return;
			} else {
				continue;
			}
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
		case JoinType::LEFT:
		case JoinType::INNER: {
			SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
			idx_t match_count =
			    NestedLoopJoinInner::Perform(state->left_tuple, state->right_tuple, state->left_join_condition,
			                                 right_chunk, lvector, rvector, conditions);
			// we have finished resolving the join conditions
			if (match_count == 0) {
				// if there are no results, move on
				continue;
			}
			// we have matching tuples!
			// construct the result
			if (state->left_found_match) {
				for (idx_t i = 0; i < match_count; i++) {
					state->left_found_match[lvector.get_index(i)] = true;
				}
			}
			chunk.Slice(state->child_chunk, lvector, match_count);
			chunk.Slice(right_data, rvector, match_count, state->child_chunk.column_count());
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
