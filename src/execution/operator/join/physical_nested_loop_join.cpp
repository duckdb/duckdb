#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/nested_loop_join.hpp"

namespace duckdb {

PhysicalNestedLoopJoin::PhysicalNestedLoopJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                               JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond), join_type, estimated_cardinality) {
	children.push_back(move(left));
	children.push_back(move(right));
}

static bool HasNullValues(DataChunk &chunk) {
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		VectorData vdata;
		chunk.data[col_idx].Orrify(chunk.size(), vdata);

		if (vdata.validity.AllValid()) {
			continue;
		}
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				return true;
			}
		}
	}
	return false;
}

template <bool MATCH>
static void ConstructSemiOrAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
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

void PhysicalJoin::ConstructSemiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<true>(left, result, found_match);
}

void PhysicalJoin::ConstructAntiJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	ConstructSemiOrAntiJoinResult<false>(left, result, found_match);
}

void PhysicalJoin::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &left, DataChunk &result, bool found_match[],
                                           bool has_null) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(left);
	for (idx_t i = 0; i < left.ColumnCount(); i++) {
		result.data[i].Reference(left.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		VectorData jdata;
		join_keys.data[col_idx].Orrify(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValid(jidx));
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
				mask.SetInvalid(i);
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class NestedLoopJoinLocalState : public LocalSinkState {
public:
	explicit NestedLoopJoinLocalState(vector<JoinCondition> &conditions) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
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
	NestedLoopJoinGlobalState() : has_null(false), right_outer_position(0) {
	}

	//! Materialized data of the RHS
	ChunkCollection right_data;
	//! Materialized join condition of the RHS
	ChunkCollection right_chunks;
	//! Whether or not the RHS of the nested loop join has NULL values
	bool has_null;
	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
	unique_ptr<bool[]> right_found_match;
	//! The position in the RHS in the final scan of the FULL OUTER JOIN
	idx_t right_outer_position;
};

void PhysicalNestedLoopJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                  DataChunk &input) {
	auto &gstate = (NestedLoopJoinGlobalState &)state;
	auto &nlj_state = (NestedLoopJoinLocalState &)lstate;

	// resolve the join expression of the right side
	nlj_state.rhs_executor.Execute(input, nlj_state.right_condition);

	// if we have not seen any NULL values yet, and we are performing a MARK join, check if there are NULL values in
	// this chunk
	if (join_type == JoinType::MARK && !gstate.has_null) {
		if (HasNullValues(nlj_state.right_condition)) {
			gstate.has_null = true;
		}
	}

	// append the data and the
	gstate.right_data.Append(input);
	gstate.right_chunks.Append(nlj_state.right_condition);
}

bool PhysicalNestedLoopJoin::Finalize(Pipeline &pipeline, ClientContext &context,
                                      unique_ptr<GlobalOperatorState> state) {
	auto &gstate = (NestedLoopJoinGlobalState &)*state;
	if (join_type == JoinType::OUTER || join_type == JoinType::RIGHT) {
		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		gstate.right_found_match = unique_ptr<bool[]>(new bool[gstate.right_data.Count()]);
		memset(gstate.right_found_match.get(), 0, sizeof(bool) * gstate.right_data.Count());
	}
	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
}

unique_ptr<GlobalOperatorState> PhysicalNestedLoopJoin::GetGlobalState(ClientContext &context) {
	return make_unique<NestedLoopJoinGlobalState>();
}

unique_ptr<LocalSinkState> PhysicalNestedLoopJoin::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<NestedLoopJoinLocalState>(conditions);
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalNestedLoopJoinState : public PhysicalOperatorState {
public:
	PhysicalNestedLoopJoinState(PhysicalOperator &op, PhysicalOperator *left, vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(op, left), fetch_next_left(true), fetch_next_right(false), right_chunk(0),
	      left_tuple(0), right_tuple(0) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
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

void PhysicalNestedLoopJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &chunk,
                                               PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalNestedLoopJoinState *>(state_p);
	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;
	do {
		// fetch the chunk to resolve
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// resolve the left join condition for the current chunk
		state->lhs_executor.Execute(state->child_chunk, state->left_condition);

		bool found_match[STANDARD_VECTOR_SIZE] = {false};
		NestedLoopJoinMark::Perform(state->left_condition, gstate.right_chunks, found_match, conditions);
		switch (join_type) {
		case JoinType::MARK:
			// now construct the mark join result from the found matches
			PhysicalJoin::ConstructMarkJoinResult(state->left_condition, state->child_chunk, chunk, found_match,
			                                      gstate.has_null);
			break;
		case JoinType::SEMI:
			// construct the semi join result from the found matches
			PhysicalJoin::ConstructSemiJoinResult(state->child_chunk, chunk, found_match);
			break;
		case JoinType::ANTI:
			// construct the anti join result from the found matches
			PhysicalJoin::ConstructAntiJoinResult(state->child_chunk, chunk, found_match);
			break;
		default:
			throw NotImplementedException("Unimplemented type for simple nested loop join!");
		}
	} while (chunk.size() == 0);
}

void PhysicalJoin::ConstructLeftJoinResult(DataChunk &left, DataChunk &result, bool found_match[]) {
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	idx_t remaining_count = 0;
	for (idx_t i = 0; i < left.size(); i++) {
		if (!found_match[i]) {
			remaining_sel.set_index(remaining_count++, i);
		}
	}
	if (remaining_count > 0) {
		result.Slice(left, remaining_sel, remaining_count);
		for (idx_t idx = left.ColumnCount(); idx < result.ColumnCount(); idx++) {
			result.data[idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[idx], true);
		}
	}
}

void PhysicalNestedLoopJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &chunk,
                                                PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalNestedLoopJoinState *>(state_p);
	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

	do {
		if (state->fetch_next_right) {
			// we exhausted the chunk on the right: move to the next chunk on the right
			state->right_chunk++;
			state->left_tuple = 0;
			state->right_tuple = 0;
			// check if we exhausted all chunks on the RHS
			state->fetch_next_left = state->right_chunk >= gstate.right_chunks.ChunkCount();
			state->fetch_next_right = false;
		}
		if (state->fetch_next_left) {
			// we exhausted all chunks on the right: move to the next chunk on the left
			if (IsLeftOuterJoin(join_type)) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				if (state->left_found_match) {
					PhysicalJoin::ConstructLeftJoinResult(state->child_chunk, chunk, state->left_found_match.get());
					state->left_found_match.reset();
					if (chunk.size() > 0) {
						return;
					}
				}
				state->left_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
				memset(state->left_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				if (join_type == JoinType::OUTER || join_type == JoinType::RIGHT) {
					// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
					// still need to output
					ConstructFullOuterJoinResult(gstate.right_found_match.get(), gstate.right_data, chunk,
					                             gstate.right_outer_position);
				}
				return;
			}
			// resolve the left join condition for the current chunk
			state->lhs_executor.Execute(state->child_chunk, state->left_condition);

			state->left_tuple = 0;
			state->right_tuple = 0;
			state->right_chunk = 0;
			state->fetch_next_left = false;
		}
		// now we have a left and a right chunk that we can join together
		// note that we only get here in the case of a LEFT, INNER or FULL join
		auto &left_chunk = state->child_chunk;
		auto &right_chunk = gstate.right_chunks.GetChunk(state->right_chunk);
		auto &right_data = gstate.right_data.GetChunk(state->right_chunk);

		// sanity check
		left_chunk.Verify();
		right_chunk.Verify();
		right_data.Verify();

		// now perform the join
		SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
		idx_t match_count = NestedLoopJoinInner::Perform(state->left_tuple, state->right_tuple, state->left_condition,
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
			if (gstate.right_found_match) {
				for (idx_t i = 0; i < match_count; i++) {
					gstate.right_found_match[state->right_chunk * STANDARD_VECTOR_SIZE + rvector.get_index(i)] = true;
				}
			}
			chunk.Slice(state->child_chunk, lvector, match_count);
			chunk.Slice(right_data, rvector, match_count, state->child_chunk.ColumnCount());
		}

		// check if we exhausted the RHS, if we did we need to move to the next right chunk in the next iteration
		if (state->right_tuple >= right_chunk.size()) {
			state->fetch_next_right = true;
		}
	} while (chunk.size() == 0);
}

void PhysicalNestedLoopJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                              PhysicalOperatorState *state_p) {
	auto state = reinterpret_cast<PhysicalNestedLoopJoinState *>(state_p);
	auto &gstate = (NestedLoopJoinGlobalState &)*sink_state;

	if (gstate.right_chunks.Count() == 0) {
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

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, chunk, state_p);
		break;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::OUTER:
	case JoinType::RIGHT:
		ResolveComplexJoin(context, chunk, state_p);
		break;
	default:
		throw NotImplementedException("Unimplemented type for nested loop join!");
	}
}

unique_ptr<PhysicalOperatorState> PhysicalNestedLoopJoin::GetOperatorState() {
	return make_unique<PhysicalNestedLoopJoinState>(*this, children[0].get(), conditions);
}

void PhysicalNestedLoopJoin::FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) {
	auto &state_p = reinterpret_cast<PhysicalNestedLoopJoinState &>(state);
	context.thread.profiler.Flush(this, &state_p.lhs_executor, "lhs_executor", 0);
	if (!children.empty() && state.child_state) {
		children[0]->FinalizeOperatorState(*state.child_state, context);
	}
}
void PhysicalNestedLoopJoin::Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	auto &state = (NestedLoopJoinLocalState &)lstate;
	context.thread.profiler.Flush(this, &state.rhs_executor, "rhs_executor", 1);
	context.client.profiler.Flush(context.thread.profiler);
}

} // namespace duckdb
