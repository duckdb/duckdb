
#include "execution/operator/join/physical_nested_loop_join_semi.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

#include "common/types/constant_vector.hpp"
#include "common/types/static_vector.hpp"

#include "common/operator/comparison_operators.hpp"

using namespace duckdb;
using namespace std;

PhysicalNestedLoopJoinSemi::PhysicalNestedLoopJoinSemi(
    LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
    vector<JoinCondition> cond, JoinType join_type)
    : PhysicalJoin(op, PhysicalOperatorType::NESTED_LOOP_JOIN, move(cond),
                   join_type) {
	children.push_back(move(left));
	children.push_back(move(right));
}

void PhysicalNestedLoopJoinSemi::_GetChunk(ClientContext &context,
                                           DataChunk &chunk,
                                           PhysicalOperatorState *state_) {
	assert(type == JoinType::SEMI || type == JoinType::ANTI);

	auto state =
	    reinterpret_cast<PhysicalNestedLoopJoinSemiOperatorState *>(state_);
	chunk.Reset();

	// first we fully materialize the right child, if we haven't done that yet
	if (state->right_chunk.column_count() == 0) {
		vector<TypeId> condition_types;
		for (auto &cond : conditions) {
			assert(cond.left->return_type == cond.right->return_type);
			condition_types.push_back(cond.left->return_type);
		}

		auto right_state = children[1]->GetOperatorState(state->parent);
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
			ExpressionExecutor executor(new_chunk, context);
			for (size_t i = 0; i < conditions.size(); i++) {
				executor.ExecuteExpression(conditions[i].right.get(),
				                           right_condition.data[i]);
			}
			// we only keep the resolved join condition. we can discard the data
			// because we never need the data from the right side for the
			// semi/anti join
			state->right_chunk.Append(right_condition);
		} while (new_chunk.size() > 0);

		if (state->right_chunk.count == 0) {
			return;
		}
		// initialize the chunks for the join conditions
		state->left_join_condition.Initialize(condition_types);
	}

	do {
		// now that we have fully materialized the right child
		// we have to perform the nested loop join
		// fetch a chunk from the left side
		children[0]->GetChunk(context, state->child_chunk,
		                      state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		state->child_chunk.Flatten();

		// resolve the left join condition for the current chunk
		state->left_join_condition.Reset();
		ExpressionExecutor executor(state->child_chunk, context);
		executor.Execute(state->left_join_condition,
		                 [&](size_t i) { return conditions[i].left.get(); },
		                 conditions.size());

		auto &left_chunk = state->child_chunk;

		// set up nop selection vector (one that selects all tuples)
		sel_t initial_vector[STANDARD_VECTOR_SIZE];
		size_t lcount = left_chunk.size();
		bool final_result[STANDARD_VECTOR_SIZE] = {false};
		for (size_t i = 0; i < left_chunk.size(); i++) {
			initial_vector[i] = i;
		}

		// now we have to resolve all the join conditions for each chunk to find
		// the matches
		for (size_t r = 0; r < state->right_chunk.chunks.size(); r++) {
			if (lcount == 0) {
				break;
			}

			auto &right_chunk = *state->right_chunk.chunks[r];

			Vector left_condition;
			left_condition.Reference(state->left_join_condition.data[0]);

			size_t left_pos = 0, right_pos = 0;
			do {
				sel_t lvector[STANDARD_VECTOR_SIZE],
				    rvector[STANDARD_VECTOR_SIZE];
				size_t match_count = 0;

				left_condition.sel_vector = initial_vector;
				left_condition.count = lcount;

				// first perform the nested loop join on the first condition
				match_count = nested_loop_join(
				    conditions[0].comparison, left_condition,
				    right_chunk.data[0], left_pos, right_pos, lvector, rvector);
				if (match_count == 0) {
					break;
				}
				// now match on the rest of the conditions
				for (size_t i = 1; i < conditions.size(); i++) {
					// for all the matches we have obtained
					// we perform the comparisons
					// get the vectors to compare
					Vector &l = state->left_join_condition.data[i];
					Vector &r = right_chunk.data[i];

					// now perform the actual comparisons
					match_count =
					    nested_loop_comparison(conditions[i].comparison, l, r,
					                           lvector, rvector, match_count);
					if (match_count == 0) {
						break;
					}
				}

				if (match_count > 0) {
					left_pos = 0;
					right_pos = 0;
					// we have matches!
					// first set the matches to true
					for (size_t i = 0; i < match_count; i++) {
						final_result[lvector[i]] = true;
					}
					// now create a new selection vector for the tuples we still
					// need to check
					lcount = 0;
					for (size_t i = 0; i < left_chunk.size(); i++) {
						if (!final_result[i]) {
							initial_vector[lcount++] = i;
						}
					}
				}
			} while (right_pos < right_chunk.data[0].count);
		}

		// now we have done all the necessary comparisons
		// create the final result

		// for ANTI join, we take every tuple that has no matches
		// for SEMI join, we take every tuple that has matches
		bool required_match = type == JoinType::ANTI ? false : true;
		size_t result_size = 0;
		for (size_t i = 0; i < left_chunk.size(); i++) {
			if (final_result[i] == required_match) {
				// this tuple belongs to the final result
				chunk.owned_sel_vector[result_size++] = i;
			}
		}
		if (result_size == 0) {
			// no result for this tuple
			// move to the next chunk from the left side
			continue;
		}

		chunk.sel_vector = chunk.owned_sel_vector;
		for (size_t i = 0; i < state->child_chunk.column_count; i++) {
			chunk.data[i].Reference(state->child_chunk.data[i]);
			chunk.data[i].count = result_size;
			chunk.data[i].sel_vector = chunk.owned_sel_vector;
		}
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalNestedLoopJoinSemi::GetOperatorState(
    ExpressionExecutor *parent_executor) {
	return make_unique<PhysicalNestedLoopJoinSemiOperatorState>(
	    children[0].get(), children[1].get(), parent_executor);
}
