#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

#include <chrono>
#include <random>
#include <vector>

using namespace duckdb;
using namespace std;

struct ConjunctionState : public ExpressionState {
	ConjunctionState(Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), iteration_count(0), observe_interval(10), execute_interval(20), warmup(true) {
		auto &conj_expr = (BoundConjunctionExpression &)expr;
		assert(conj_expr.children.size() > 1);
		for (idx_t idx = 0; idx < conj_expr.children.size(); idx++) {
			permutation.push_back(idx);
			if (idx != conj_expr.children.size() - 1) {
				swap_likeliness.push_back(100);
			}
		}
		right_random_border = 100 * (conj_expr.children.size() - 1);
	}

	// used for adaptive expression reordering
	idx_t iteration_count;
	idx_t swap_idx;
	idx_t right_random_border;
	idx_t observe_interval;
	idx_t execute_interval;
	double runtime_sum;
	double prev_mean;
	bool observe;
	bool warmup;
	vector<idx_t> permutation;
	vector<idx_t> swap_likeliness;
	std::default_random_engine generator;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundConjunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ConjunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	return move(result);
}

void ExpressionExecutor::Execute(BoundConjunctionExpression &expr, ExpressionState *state, Vector &result, idx_t count) {
	// execute the children
	for (idx_t i = 0; i < expr.children.size(); i++) {
		Vector current_result(TypeId::BOOL);
		Execute(*expr.children[i], state->child_states[i].get(), current_result, count);
		if (i == 0) {
			// move the result
			result.Reference(current_result);
		} else {
			Vector intermediate(TypeId::BOOL);
			// AND/OR together
			switch (expr.type) {
			case ExpressionType::CONJUNCTION_AND:
				VectorOperations::And(current_result, result, intermediate, count);
				break;
			case ExpressionType::CONJUNCTION_OR:
				VectorOperations::Or(current_result, result, intermediate, count);
				break;
			default:
				throw NotImplementedException("Unknown conjunction type!");
			}
			result.Reference(intermediate);
		}
	}
}

static void MergeSelectionVectorIntoResult(sel_t *result, idx_t &result_count, sel_t *sel, idx_t count) {
	assert(count > 0);
	if (result_count == 0) {
		// nothing to merge
		memcpy(result, sel, count * sizeof(sel_t));
		result_count = count;
		return;
	}

	sel_t temp_result[STANDARD_VECTOR_SIZE];
	idx_t res_idx = 0, sel_idx = 0;
	idx_t temp_count = 0;
	while (true) {
		// the two sets should be disjunct
		assert(result[res_idx] != sel[sel_idx]);
		if (result[res_idx] < sel[sel_idx]) {
			temp_result[temp_count++] = result[res_idx];
			res_idx++;
			if (res_idx >= result_count) {
				break;
			}
		} else {
			assert(result[res_idx] > sel[sel_idx]);
			temp_result[temp_count++] = sel[sel_idx];
			sel_idx++;
			if (sel_idx >= count) {
				break;
			}
		}
	}
	// append remaining entries
	if (sel_idx < count) {
		// first copy the temp_result to the result
		memcpy(result, temp_result, temp_count * sizeof(sel_t));
		// now copy the remaining entries in the selection vector after the initial result
		memcpy(result + temp_count, sel + sel_idx, (count - sel_idx) * sizeof(sel_t));
		result_count = temp_count + count - sel_idx;
	} else {
		// first copy the remainder of the result into the temp_result vector
		memcpy(temp_result + temp_count, result + res_idx, (result_count - res_idx) * sizeof(sel_t));
		result_count = temp_count + (result_count - res_idx);
		// now copy the temp_result back into the main result vector
		memcpy(result, temp_result, result_count * sizeof(sel_t));
	}
}

void AdaptRuntimeStatistics(BoundConjunctionExpression &expr, ConjunctionState *state, double duration) {
	state->iteration_count++;
	state->runtime_sum += duration;

	if (!state->warmup) {
		// the last swap was observed
		if (state->observe && state->iteration_count == state->observe_interval) {
			// keep swap if runtime decreased, else reverse swap
			if (!(state->prev_mean - (state->runtime_sum / state->iteration_count) > 0)) {
				// reverse swap because runtime didn't decrease
				assert(state->swap_idx < expr.children.size() - 1);
				assert(expr.children.size() > 1);
				swap(state->permutation[state->swap_idx], state->permutation[state->swap_idx + 1]);

				// decrease swap likeliness, but make sure there is always a small likeliness left
				if (state->swap_likeliness[state->swap_idx] > 1) {
					state->swap_likeliness[state->swap_idx] /= 2;
				}
			} else {
				// keep swap because runtime decreased, reset likeliness
				state->swap_likeliness[state->swap_idx] = 100;
			}
			state->observe = false;

			// reset values
			state->iteration_count = 0;
			state->runtime_sum = 0.0;
		} else if (!state->observe && state->iteration_count == state->execute_interval) {
			// save old mean to evaluate swap
			state->prev_mean = state->runtime_sum / state->iteration_count;

			// get swap index and swap likeliness
			uniform_int_distribution<int> distribution(1, state->right_random_border); // a <= i <= b
			idx_t random_number = distribution(state->generator) - 1;

			state->swap_idx = random_number / 100;                    // index to be swapped
			idx_t likeliness = random_number - 100 * state->swap_idx; // random number between [0, 100)

			// check if swap is going to happen
			if (state->swap_likeliness[state->swap_idx] > likeliness) { // always true for the first swap of an index
				// swap
				assert(state->swap_idx < expr.children.size() - 1);
				assert(expr.children.size() > 1);
				swap(state->permutation[state->swap_idx], state->permutation[state->swap_idx + 1]);

				// observe whether swap will be applied
				state->observe = true;
			}

			// reset values
			state->iteration_count = 0;
			state->runtime_sum = 0.0;
		}
	} else {
		if (state->iteration_count == 5) {
			// initially set all values
			state->iteration_count = 0;
			state->runtime_sum = 0.0;
			state->observe = false;
			state->warmup = false;
		}
	}
}

idx_t ExpressionExecutor::Select(BoundConjunctionExpression &expr, ExpressionState *state_, idx_t count, SelectionVector &true_sel, SelectionVector &false_sel) {
	auto state = (ConjunctionState *)state_;
	if (!chunk) {
		return DefaultSelect(expr, state, count, true_sel, false_sel);
	}

	chrono::time_point<chrono::high_resolution_clock> start_time;
	chrono::time_point<chrono::high_resolution_clock> end_time;

	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		throw NotImplementedException("FIXME AND");

		// // store the initial selection vector and count
		// auto initial_sel = chunk->sel_vector;
		// idx_t initial_count = chunk->size();
		// idx_t current_count = chunk->size();

		// // get runtime statistics
		// start_time = chrono::high_resolution_clock::now();

		// for (idx_t i = 0; i < expr.children.size(); i++) {

		// 	// first resolve the current expression and get its execution time
		// 	idx_t new_count =
		// 	    Select(*expr.children[state->permutation[i]], state->child_states[state->permutation[i]].get(), result);

		// 	if (new_count == 0) {
		// 		current_count = 0;
		// 		break;
		// 	}
		// 	if (new_count != current_count) {
		// 		// disqualify all non-qualifying tuples by updating the selection vector
		// 		chunk->SetCardinality(new_count, result);
		// 		current_count = new_count;
		// 	}
		// }

		// // adapt runtime statistics
		// end_time = chrono::high_resolution_clock::now();
		// AdaptRuntimeStatistics(expr, state,
		//                        chrono::duration_cast<chrono::duration<double>>(end_time - start_time).count());

		// // restore the initial selection vector and count
		// chunk->SetCardinality(initial_count, initial_sel);
		// return current_count;
	} else {
		throw NotImplementedException("FIXME OR");
		// sel_t *initial_sel = chunk->sel_vector;
		// idx_t initial_count = chunk->size();
		// idx_t current_count = chunk->size();
		// sel_t *current_sel = initial_sel;

		// sel_t intermediate_result[STANDARD_VECTOR_SIZE];
		// sel_t expression_result[STANDARD_VECTOR_SIZE];
		// sel_t remaining[STANDARD_VECTOR_SIZE];
		// idx_t result_count = 0;
		// idx_t remaining_count = 0;
		// sel_t *result_vector = initial_sel == result ? intermediate_result : result;

		// // get runtime statistics
		// start_time = chrono::high_resolution_clock::now();

		// for (idx_t expr_idx = 0; expr_idx < expr.children.size(); expr_idx++) {
		// 	// first resolve the current expression
		// 	idx_t new_count = Select(*expr.children[state->permutation[expr_idx]],
		// 	                         state->child_states[state->permutation[expr_idx]].get(), expression_result);
		// 	if (new_count == 0) {
		// 		// no new qualifying entries: continue
		// 		continue;
		// 	}
		// 	if (new_count == current_count) {
		// 		// all remaining entries qualified! add them to the result
		// 		if (!current_sel) {
		// 			// first iteration already passes all tuples, no need to set up selection vector
		// 			assert(current_count == initial_count);
		// 			result_count = initial_count;
		// 			break;
		// 		}
		// 		MergeSelectionVectorIntoResult(result_vector, result_count, current_sel, current_count);
		// 		break;
		// 	}
		// 	// first merge the current results back into the result vector
		// 	MergeSelectionVectorIntoResult(result_vector, result_count, expression_result, new_count);
		// 	if (expr_idx + 1 == expr.children.size()) {
		// 		// this is the last child: we don't need to construct the remaining tuples
		// 		break;
		// 	}
		// 	// now we only need to continue executing tuples that were not qualified
		// 	// we figure this out by performing a merge of the remaining tuples and the resulting selection vector
		// 	idx_t new_idx = 0;
		// 	remaining_count = 0;
		// 	for (idx_t i = 0; i < current_count; i++) {
		// 		auto entry = current_sel ? current_sel[i] : i;
		// 		if (new_idx >= new_count || expression_result[new_idx] != entry) {
		// 			remaining[remaining_count++] = entry;
		// 		} else {
		// 			new_idx++;
		// 		}
		// 	}
		// 	current_sel = remaining;
		// 	current_count = remaining_count;
		// 	chunk->SetCardinality(remaining_count, remaining);
		// }

		// // adapt runtime statistics
		// end_time = chrono::high_resolution_clock::now();
		// AdaptRuntimeStatistics(expr, state,
		//                        chrono::duration_cast<chrono::duration<double>>(end_time - start_time).count());

		// chunk->SetCardinality(initial_count, initial_sel);
		// if (result_vector != result && result_count > 0) {
		// 	memcpy(result, result_vector, result_count * sizeof(sel_t));
		// }
		// return result_count;
	}
}
