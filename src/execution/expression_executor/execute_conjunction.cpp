#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor/bitmap_comparison.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/adaptive_filter.hpp"

#include <random>

namespace duckdb {

struct ConjunctionState : public ExpressionState {
	ConjunctionState(const Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), intersect_tmp(STANDARD_VECTOR_SIZE) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			dense_child.push_back(IsBitmapSelectCandidate(*child));
			bitmap_capable = bitmap_capable || dense_child.back();
		}
		bitmap_capable = bitmap_capable && CpuBenefitsFromAutoVec();
		adaptive_filter = make_uniq<AdaptiveFilter>(expr);
		if (HasContext()) {
			adaptive_filter->SetLogger(GetContext().logger);
		}
	}
	unique_ptr<AdaptiveFilter> adaptive_filter;
	SelectionResult intersect_acc, intersect_tmp; // bitmap accumulator & per-child scratch
	bool bitmap_capable = false;                  // at least one child can produce a bitmap
	vector<bool> dense_child;                     // cached per-child bitmap eligibility
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundConjunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ConjunctionState>(expr, root);
	for (auto &child : expr.GetChildren()) {
		result->AddChild(*child);
	}

	result->Finalize();
	return std::move(result);
}

void ExpressionExecutor::Execute(const BoundConjunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// execute the children
	state->intermediate_chunk.Reset();
	for (idx_t i = 0; i < expr.GetChildren().size(); i++) {
		auto &current_result = state->intermediate_chunk.data[i];
		Execute(*expr.GetChildren()[i], state->child_states[i].get(), sel, count, current_result);
		if (i == 0) {
			// move the result
			result.Reference(current_result);
		} else {
			Vector intermediate(LogicalType::BOOLEAN);
			// AND/OR together
			switch (expr.GetExpressionType()) {
			case ExpressionType::CONJUNCTION_AND:
				VectorOperations::And(current_result, result, intermediate);
				break;
			case ExpressionType::CONJUNCTION_OR:
				VectorOperations::Or(current_result, result, intermediate);
				break;
			default:
				throw InternalException("Unknown conjunction type!");
			}
			result.Reference(intermediate);
		}
	}
}

idx_t ExpressionExecutor::Select(const BoundConjunctionExpression &expr, ExpressionState *state_p,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel, SelectionResult *bitmap_sel = nullptr) {
	auto &state = state_p->Cast<ConjunctionState>();
	const bool is_and = expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND;
	if (state.bitmap_capable && AutoVecCountPaysOff(count) && true_sel && !false_sel &&
	    (!sel || !sel->IsSet())) { // bitmap AND/OR fast path
		auto &children = expr.GetChildren();
		idx_t result_count = is_and ? count : 0;
		bool have_accumulator = false;
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child = *children[child_idx];
			auto child_state = state.child_states[child_idx].get();
			const bool dense = state.dense_child[child_idx]; // dense children combine over the full vector
			const SelectionVector *current_sel = nullptr;
			idx_t current_count = count;
			if (!is_and && !dense) {
				state.bitmap_capable = false;
				have_accumulator = false;
				break;
			}
			const bool narrow = have_accumulator && // leave the dense path once few rows survive:
			                    (!dense || (is_and && !DenseAutoVecPaysOff(result_count, count, sizeof(int64_t))));
			if (narrow) {
				current_sel = &state.intersect_acc.Flattened();
				current_count = result_count;
			}
			state.intersect_tmp.EnsureIndexWritable(count);
			idx_t child_count =
			    Select(child, child_state, current_sel, current_count, nullptr, nullptr, &state.intersect_tmp);
			state.intersect_tmp.ToBitmap(child_count, count);
			if (have_accumulator && dense && !narrow) { // a narrowed child already saw the accumulator's survivors
				result_count =
				    is_and ? state.intersect_acc.Intersect(state.intersect_tmp, result_count, child_count, count)
				           : state.intersect_acc.Union(state.intersect_tmp);
			} else {
				std::swap(state.intersect_acc, state.intersect_tmp);
				result_count = child_count;
			}
			have_accumulator = true;
			if ((is_and && result_count == 0) || (!is_and && result_count == count)) {
				break;
			}
		}
		// every early exit either clears the accumulator or leaves an empty (AND) / full (OR) result
		if (have_accumulator) {
			if (bitmap_sel) {
				std::swap(*bitmap_sel, state.intersect_acc); // keep old caller buffers for scratch reuse
			} else {
				state.intersect_acc.SwapInto(*true_sel);
				true_sel->Flatten(); // plain callers need an index selection
			}
			return result_count;
		}
		// no accumulator: the loop bailed on a non-dense OR child, which already cleared bitmap_capable
	}

	if (is_and) {
		// get runtime statistics
		auto filter_state = state.adaptive_filter->BeginFilter();
		const auto &permutation = state.adaptive_filter->GetPermutation();
		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t false_count = 0;

		unique_ptr<SelectionVector> temp_true, temp_false;
		if (false_sel) {
			temp_false = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!true_sel) {
			temp_true = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			true_sel = temp_true.get();
		}
		for (idx_t i = 0; i < expr.GetChildren().size(); i++) {
			idx_t tcount = Select(*expr.GetChildren()[permutation[i]], state.child_states[permutation[i]].get(),
			                      current_sel, current_count, true_sel, temp_false.get());
			idx_t fcount = current_count - tcount;
			if (fcount > 0 && false_sel) {
				// move failing tuples into the false_sel
				// tuples passed, move them into the actual result vector
				for (idx_t i = 0; i < fcount; i++) {
					false_sel->set_index(false_count++, temp_false->get_index(i));
				}
			}
			current_count = tcount;
			if (current_count == 0) {
				break;
			}
			if (current_count < count) {
				// tuples were filtered out: move on to using the true_sel to only evaluate passing tuples in subsequent
				// iterations
				current_sel = true_sel;
			}
		}
		// adapt runtime statistics
		state.adaptive_filter->EndFilter(filter_state);
		return current_count;
	} else {
		// get runtime statistics
		auto filter_state = state.adaptive_filter->BeginFilter();
		const auto &permutation = state.adaptive_filter->GetPermutation();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t result_count = 0;

		unique_ptr<SelectionVector> temp_true, temp_false;
		if (true_sel) {
			temp_true = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!false_sel) {
			temp_false = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			false_sel = temp_false.get();
		}
		for (idx_t i = 0; i < expr.GetChildren().size(); i++) {
			idx_t tcount = Select(*expr.GetChildren()[permutation[i]], state.child_states[permutation[i]].get(),
			                      current_sel, current_count, temp_true.get(), false_sel);
			if (tcount > 0) {
				if (true_sel) {
					// tuples passed, move them into the actual result vector
					for (idx_t i = 0; i < tcount; i++) {
						true_sel->set_index(result_count++, temp_true->get_index(i));
					}
				}
				// now move on to check only the non-passing tuples
				current_count -= tcount;
				current_sel = false_sel;
			}
		}
		if (true_sel) {
			true_sel->Sort(result_count);
		}

		// adapt runtime statistics
		state.adaptive_filter->EndFilter(filter_state);
		return result_count;
	}
}

} // namespace duckdb
