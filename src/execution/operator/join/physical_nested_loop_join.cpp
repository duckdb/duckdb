#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/nested_loop_join.hpp"

using namespace std;

namespace duckdb {

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

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NestedLoopJoinLocalState : public LocalSinkState {
public:
	NestedLoopJoinLocalState(vector<JoinCondition> &conditions) {
		vector<TypeId> condition_types;
		for(auto &cond : conditions) {
			rhs_executor.AddExpression(*cond.right);
			condition_types.push_back(cond.right->return_type);
		}
		right_condition.Initialize(condition_types);
	}

	//! The chunk holding the right condition
	DataChunk right_condition;
	//! The executor of the RHS condition
	ExpressionExecutor rhs_executor;
};

class NestedLoopJoinGlobalState : public GlobalOperatorState {
public:
	NestedLoopJoinGlobalState() : has_null(false) {
	}

	//! Materialized data of the RHS
	ChunkCollection right_data;
	//! Materialized join condition of the RHS
	ChunkCollection right_chunks;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;
};

void PhysicalNestedLoopJoin::Sink(ClientContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) {
	auto &gstate = (NestedLoopJoinGlobalState &) state;
	auto &nlj_state = (NestedLoopJoinLocalState &) lstate;

	// resolve the join expression of the right side
	nlj_state.rhs_executor.Execute(input, nlj_state.right_condition);

	// if we have not seen any NULL values yet, and we are performing a MARK join, check if there are NULL values in this chunk
	if (join_type == JoinType::MARK && !gstate.has_null) {
		if (HasNullValues(nlj_state.right_condition)) {
			gstate.has_null = true;
		}
	}

	// append the data and the
	gstate.right_data.Append(input);
	gstate.right_chunks.Append(nlj_state.right_condition);
}

unique_ptr<GlobalOperatorState> PhysicalNestedLoopJoin::GetGlobalState(ClientContext &context) {
	return make_unique<NestedLoopJoinGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalNestedLoopJoin::GetLocalSinkState(ClientContext &context) {
	return make_unique<NestedLoopJoinLocalState>(conditions);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalNestedLoopJoinState : public PhysicalOperatorState {
public:
	PhysicalNestedLoopJoinState(PhysicalOperator *left, vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(left), fetch_next_left(true), fetch_next_right(false), right_chunk(0), left_tuple(0), right_tuple(0) {
		vector<TypeId> condition_types;
		for(auto &cond : conditions) {
			lhs_executor.AddExpression(*cond.left);
			condition_types.push_back(cond.left->return_type);
		}
		left_condition.Initialize(condition_types);
	}

	bool fetch_next_left;
	bool fetch_next_right;
	idx_t right_chunk;
	DataChunk left_condition;
	//! The executor of the LHS condition
	ExpressionExecutor lhs_executor;

	idx_t left_tuple;
	idx_t right_tuple;

	unique_ptr<bool[]> left_found_match;
};

void PhysicalNestedLoopJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalNestedLoopJoinState *>(state_);
	auto &gstate = (NestedLoopJoinGlobalState &) *sink_state;

	if (gstate.right_chunks.count == 0) {
		// empty RHS
		if (join_type == JoinType::SEMI || join_type == JoinType::INNER) {
			// for SEMI or INNER join: empty RHS means empty result
			return;
		}
		// pull a chunk from the LHS
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		ConstructEmptyJoinResult(join_type, gstate.has_null, state->child_chunk, chunk);
		return;
	}

	do {
		if (state->fetch_next_right) {
			// we exhausted the chunk on the right: move to the next chunk on the right
			state->right_chunk++;
			state->left_tuple = 0;
			state->right_tuple = 0;
			// check if we exhausted all chunks on the RHS
			state->fetch_next_left = state->right_chunk >= gstate.right_chunks.chunks.size();
			state->fetch_next_right = false;
		}
		if (state->fetch_next_left) {
			// we exhausted all chunks on the right: move to the next chunk on the left
			do {
				if (join_type == JoinType::LEFT) {
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
				state->lhs_executor.Execute(state->child_chunk, state->left_condition);

				// MARK, SEMI and ANTI joins are handled separately because they scan the whole RHS in one go
				// we can resolve them here in their entirety
				switch (join_type) {
				case JoinType::SEMI:
				case JoinType::ANTI:
				case JoinType::MARK: {
					bool found_match[STANDARD_VECTOR_SIZE] = {false};
					NestedLoopJoinMark::Perform(state->left_condition, gstate.right_chunks, found_match, conditions);
					if (join_type == JoinType::MARK) {
						// now construct the mark join result from the found matches
						PhysicalJoin::ConstructMarkJoinResult(state->left_condition, state->child_chunk, chunk,
															found_match, gstate.has_null);
					} else if (join_type == JoinType::SEMI) {
						// construct the semi join result from the found matches
						PhysicalJoin::ConstructSemiOrAntiJoinResult<true>(state->child_chunk, chunk, found_match);
					} else if (join_type == JoinType::ANTI) {
						PhysicalJoin::ConstructSemiOrAntiJoinResult<false>(state->child_chunk, chunk, found_match);
					}
					// move to the next LHS chunk in the next iteration
					state->right_tuple = gstate.right_chunks.chunks[state->right_chunk]->size();
					state->right_chunk = gstate.right_chunks.chunks.size() - 1;
					if (chunk.size() > 0) {
						return;
					} else {
						continue;
					}
				}
				default:
					break;
				}
			} while (state->left_condition.size() == 0);

			state->left_tuple = 0;
			state->right_tuple = 0;
			state->right_chunk = 0;
			state->fetch_next_left = false;
		}
		// now we have a left and a right chunk that we can join together
		// note that we only get here in the case of a LEFT, INNER or FULL join
		auto &left_chunk = state->child_chunk;
		auto &right_chunk = *gstate.right_chunks.chunks[state->right_chunk];
		auto &right_data = *gstate.right_data.chunks[state->right_chunk];

		// sanity check
		left_chunk.Verify();
		right_chunk.Verify();
		right_data.Verify();

		// now perform the join
		switch (join_type) {
		case JoinType::LEFT:
		case JoinType::INNER: {
			SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
			idx_t match_count =
			    NestedLoopJoinInner::Perform(state->left_tuple, state->right_tuple, state->left_condition,
			                                 right_chunk, lvector, rvector, conditions);
			// we have finished resolving the join conditions
			if (match_count > 0) {
				// we have matching tuples!
				// construct the result
				if (state->left_found_match) {
					for (idx_t i = 0; i < match_count; i++) {
						state->left_found_match[lvector.get_index(i)] = true;
					}
				}
				chunk.Slice(state->child_chunk, lvector, match_count);
				chunk.Slice(right_data, rvector, match_count, state->child_chunk.column_count());
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for nested loop join!");
		}
		// check if we exhausted the RHS, if we did we need to move to the next right chunk in the next iteration
		if (state->right_tuple >= right_chunk.size()) {
			state->fetch_next_right = true;
		}
	} while(chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalNestedLoopJoin::GetOperatorState() {
	return make_unique<PhysicalNestedLoopJoinState>(children[0].get(), conditions);
}

} // namespace duckdb
