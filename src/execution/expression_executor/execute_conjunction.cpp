#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor/bitmap_comparison.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/adaptive_filter.hpp"

#include <random>

namespace duckdb {

struct ConjunctionState : public ExpressionState {
	ConjunctionState(const Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), intersect_tmp(STANDARD_VECTOR_SIZE),
	      bitmap_capable(HasBitmapComparisonChild(expr.Cast<BoundConjunctionExpression>())) {
		adaptive_filter = make_uniq<AdaptiveFilter>(expr);
		if (HasContext()) {
			adaptive_filter->SetLogger(GetContext().logger);
		}
	}
	unique_ptr<AdaptiveFilter> adaptive_filter;
	//! Scratches for the AND bitmap-intersect fast path: acc accumulates, tmp holds each subsequent child
	SelectionResult intersect_acc;
	SelectionResult intersect_tmp;
	//! Only enabled when the conjunction has at least one child that can be evaluated densely into a bitmap
	bool bitmap_capable;
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

	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		// Bitmap intersect fast path: keep one bitmap accumulator. Non-bitmap children execute once on the
		// current accumulator (materialized), then their index result is promoted back to a bitmap; bitmap-safe
		// comparison children can execute over the dense input and be AND-ed into the accumulator.
		if (state.bitmap_capable && true_sel && !false_sel && (!sel || !sel->IsSet())) {
			auto &children = expr.GetChildren();
			idx_t intersect_count = count;
			bool have_accumulator = false;
			bool used_dense_bitmap_child = false;

			for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
				auto &child = *children[child_idx];
				auto child_state = state.child_states[child_idx].get();
				// dense candidates evaluate over the full input and are AND-ed into the accumulator; other
				// children narrow on the (materialized) accumulator and their result replaces it
				const bool dense = IsBitmapComparisonCandidate(child);
				const SelectionVector *current_sel = nullptr;
				idx_t current_count = count;
				if (!dense && have_accumulator) {
					state.intersect_acc.Flatten();
					current_sel = &state.intersect_acc;
					current_count = intersect_count;
				}
				state.intersect_tmp.EnsureIndexWritable(count);
				idx_t child_count = Select(child, child_state, current_sel, current_count, &state.intersect_tmp,
				                           nullptr, &state.intersect_tmp);
				state.intersect_tmp.ToBitmap(child_count, count);
				if (dense && have_accumulator) {
					child_count =
					    state.intersect_acc.Intersect(state.intersect_tmp, intersect_count, child_count, count);
				} else {
					std::swap(state.intersect_acc, state.intersect_tmp);
					have_accumulator = true;
				}
				used_dense_bitmap_child |= dense;

				intersect_count = child_count;
				if (intersect_count == 0) {
					break;
				}
			}

			if (have_accumulator && (used_dense_bitmap_child || intersect_count == 0)) {
				// swap, not move: the scratch keeps true_sel's old buffers for reuse on the next vector
				std::swap(*true_sel, static_cast<SelectionVector &>(state.intersect_acc));
				if (bitmap_sel != true_sel) {
					// the caller's output is a plain SelectionVector: materialize the bitmap once
					true_sel->Flatten();
				}
				return intersect_count;
			}
			state.bitmap_capable = false;
		}

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
