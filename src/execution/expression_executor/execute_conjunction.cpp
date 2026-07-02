#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/adaptive_filter.hpp"

#include <random>

namespace duckdb {

// Ready a reused bitmap-fast-path scratch selection to receive up to `count` values from a child Select. A
// bitmap-backed or empty scratch grows itself on the next write, but a leftover undersized index buffer (from
// an earlier, smaller vector) would be written out of bounds, so clear it and let the child re-provision.
static inline void PrepareScratch(SelectionVector &scratch, idx_t count) {
	if (!scratch.IsBitmap() && scratch.Capacity() < count) {
		scratch = SelectionVector();
	}
}

struct ConjunctionState : public ExpressionState {
	ConjunctionState(const Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), intersect_tmp(STANDARD_VECTOR_SIZE) {
		adaptive_filter = make_uniq<AdaptiveFilter>(expr);
		if (HasContext()) {
			adaptive_filter->SetLogger(GetContext().logger);
		}
	}
	unique_ptr<AdaptiveFilter> adaptive_filter;
	//! Scratch for the AND bitmap-intersect fast path: `intersect_acc` accumulates, `intersect_tmp` holds
	//! each subsequent child. Kept off `true_sel` so a fall-through leaves it untouched for the normal path.
	SelectionVector intersect_acc;
	SelectionVector intersect_tmp;
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
                                 SelectionVector *false_sel) {
	auto &state = state_p->Cast<ConjunctionState>();

	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		// Bitmap intersect fast path: over the identity selection, with only the positive selection wanted,
		// evaluate each child over the full input and AND the resulting bitmaps directly into true_sel. As
		// long as every child yields a bitmap the result stays a bitmap with no index materialization;
		// otherwise fall back (the normal path below overwrites true_sel).
		if (true_sel && !false_sel && (!sel || !sel->IsSet())) {
			auto &children = expr.GetChildren();
			// The scratch vectors are pure output targets, but a child may fill one with up to `count` indices
			// (e.g. an all-pass filter) rather than a bitmap. set_index only auto-grows an empty/bitmap-backed
			// selection, not an undersized index buffer left over from a smaller vector, so make sure each scratch
			// can hold `count` before use (a bitmap-producing child re-allocates its own storage regardless).
			PrepareScratch(state.intersect_acc, count);
			// Accumulate into scratch, not true_sel, so a fall-through leaves true_sel for the normal path.
			idx_t intersect_count =
			    Select(*children[0], state.child_states[0].get(), nullptr, count, &state.intersect_acc, nullptr);
			if (state.intersect_acc.IsBitmap()) {
				bool all_bitmap = true;
				for (idx_t i = 1; i < children.size(); i++) {
					PrepareScratch(state.intersect_tmp, count);
					Select(*children[i], state.child_states[i].get(), nullptr, count, &state.intersect_tmp, nullptr);
					if (!state.intersect_tmp.IsBitmap()) {
						all_bitmap = false;
						break;
					}
					intersect_count = state.intersect_acc.IntersectBitmap(state.intersect_tmp);
				}
				if (all_bitmap) {
					*true_sel = std::move(state.intersect_acc);
					return intersect_count;
				}
			}
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
