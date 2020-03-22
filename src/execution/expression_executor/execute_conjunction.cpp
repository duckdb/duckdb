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

void ExpressionExecutor::Execute(BoundConjunctionExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count, Vector &result) {
	// execute the children
	for (idx_t i = 0; i < expr.children.size(); i++) {
		Vector current_result(TypeId::BOOL);
		Execute(*expr.children[i], state->child_states[i].get(), sel, count, current_result);
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

idx_t ExpressionExecutor::Select(BoundConjunctionExpression &expr, ExpressionState *state_, const SelectionVector *sel, idx_t count, SelectionVector &true_sel, SelectionVector &false_sel) {
	auto state = (ConjunctionState *)state_;

	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		// get runtime statistics
		auto start_time = chrono::high_resolution_clock::now();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;

		for (idx_t i = 0; i < expr.children.size(); i++) {
			current_count = Select(*expr.children[state->permutation[i]], state->child_states[state->permutation[i]].get(), current_sel, current_count, true_sel, false_sel);
			if (current_count == 0) {
				break;
			}
			if (current_count < count) {
				// tuples were filtered out: move on to using the true_sel to only evaluate passing tuples in subsequent iterations
				current_sel = &true_sel;
			}
		}

		// adapt runtime statistics
		auto end_time = chrono::high_resolution_clock::now();
		AdaptRuntimeStatistics(expr, state, chrono::duration_cast<chrono::duration<double>>(end_time - start_time).count());
		return current_count;
	} else {
		// get runtime statistics
		auto start_time = chrono::high_resolution_clock::now();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t result_count = 0;

		SelectionVector temp_true(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < expr.children.size(); i++) {
			idx_t tcount = Select(*expr.children[state->permutation[i]], state->child_states[state->permutation[i]].get(), current_sel, current_count, temp_true, false_sel);
			if (tcount > 0) {
				// tuples passed, move them into the actual result vector
				for(idx_t i = 0; i < tcount; i++) {
					true_sel.set_index(result_count++, temp_true.get_index(i));
				}
				// now move on to check only the non-passing tuples
				current_count -= tcount;
				current_sel = &false_sel;
				break;
			}
		}

		// adapt runtime statistics
		auto end_time = chrono::high_resolution_clock::now();
		AdaptRuntimeStatistics(expr, state, chrono::duration_cast<chrono::duration<double>>(end_time - start_time).count());
		return result_count;
	}
}
