#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/constant_filter_combiner.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

static bool ScanInImpliesComparison(const BoundOperatorExpression &in, const ConstantComparison &bound) {
	for (idx_t i = 1; i < in.GetChildren().size(); i++) {
		auto &member = *in.GetChildren()[i];
		auto &value = member.Cast<BoundConstantExpression>().GetValue();
		if (value.type() != bound.constant.type()) {
			return false;
		}
		// NULL is not a matching member of IN in a filter.
		if (value.IsNull()) {
			continue;
		}
		ConstantComparison equality {value, ExpressionType::COMPARE_EQUAL};
		if (ConstantFilterCombiner::Compare(equality, bound) != ValueComparisonResult::PRUNE_RIGHT) {
			return false;
		}
	}
	return true;
}

struct ScanBound : ConstantComparison {
	ScanBound(const Expression &source_p, bool required_p) : source(source_p), required(required_p) {
	}

	reference<const Expression> source;
	bool required;
};

static FilterResult CombineScanComparison(const Expression &candidate, optional_ptr<const Expression> optional_child,
                                          expression_map_t<vector<ScanBound>> &bounds,
                                          reference_set_t<const Expression> &removed) {
	auto &expr = optional_child ? *optional_child : candidate;
	if (!ConstantFilterCombiner::IsSupportedComparison(expr.GetExpressionType())) {
		return FilterResult::SUCCESS;
	}
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	const bool flipped = left.GetExpressionType() == ExpressionType::VALUE_CONSTANT;
	auto &target = flipped ? right : left;
	auto &constant_expr = flipped ? left : right;
	if (constant_expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT ||
	    target.GetExpressionType() == ExpressionType::VALUE_CONSTANT || !FilterCombiner::CanMatchExpression(target)) {
		return FilterResult::SUCCESS;
	}
	auto &constant = constant_expr.Cast<BoundConstantExpression>().GetValue();
	// Nested Value ordering (notably VARIANT) can differ from the expression's execution.
	if (target.GetReturnType() != constant.type() || constant.type().IsNested()) {
		return FilterResult::SUCCESS;
	}
	// Only promote cheap native comparisons; arbitrary optional predicates keep their execution policy.
	if (optional_child && (!ExpressionFilter::IsSimpleFilterColumnRef(target) ||
	                       !ExpressionFilter::SupportsConstantFilterPushdown(constant.type()) || constant.IsNull())) {
		return FilterResult::SUCCESS;
	}
	auto &required_bounds = bounds[target];
	if (optional_child && required_bounds.empty()) {
		return FilterResult::SUCCESS;
	}
	ScanBound bound(candidate, !optional_child);
	bound.constant = constant;
	bound.comparison_type =
	    flipped ? FlipComparisonExpression(comparison.GetExpressionType()) : comparison.GetExpressionType();
	auto result = ConstantFilterCombiner::AddComparison(
	    required_bounds, std::move(bound), [&removed](ScanBound &retained, const ScanBound &removed_bound) {
		    retained.required |= removed_bound.required;
		    removed.insert(removed_bound.source);
	    });
	if (result == FilterResult::SUCCESS && !required_bounds.back().required) {
		// This optional bound did not replace a required bound. Leave it optional and out of the solver.
		required_bounds.pop_back();
	}
	return result;
}

static FilterResult CombineScanComparisons(const vector<reference<const Expression>> &candidates,
                                           reference_set_t<const Expression> &removed,
                                           reference_set_t<const Expression> &promoted) {
	expression_map_t<vector<ScanBound>> bounds;
	// Establish the required bounds first; optional predicates only join this set when they replace one.
	for (bool optional : {false, true}) {
		for (auto &candidate_ref : candidates) {
			auto &candidate = candidate_ref.get();
			auto optional_child = ExpressionFilter::GetOptionalFilterChild(candidate);
			if (bool(optional_child) != optional) {
				continue;
			}
			if (CombineScanComparison(candidate, optional_child, bounds, removed) == FilterResult::UNSATISFIABLE) {
				return FilterResult::UNSATISFIABLE;
			}
		}
	}
	// Only an already-required IN can replace bounds. Optional IN keeps its cheaper pruning/bypass policy.
	for (auto &candidate : candidates) {
		auto &expr = candidate.get();
		if (!ExpressionFilter::IsSmallNativeInFilter(expr)) {
			continue;
		}
		auto &in = expr.Cast<BoundOperatorExpression>();
		if (!FilterCombiner::CanMatchExpression(*in.GetChildren()[0])) {
			continue;
		}
		auto entry = bounds.find(*in.GetChildren()[0]);
		if (entry == bounds.end()) {
			continue;
		}
		auto &required_bounds = entry->second;
		for (idx_t i = 0; i < required_bounds.size(); i++) {
			if (ScanInImpliesComparison(in, required_bounds[i])) {
				removed.insert(required_bounds[i].source);
				required_bounds.erase_at(i--);
			}
		}
	}
	for (auto &entry : bounds) {
		for (auto &bound : entry.second) {
			auto &candidate = bound.source.get();
			if (ExpressionFilter::GetOptionalFilterChild(candidate)) {
				promoted.insert(candidate);
			}
		}
	}
	return FilterResult::SUCCESS;
}

static bool CanReorderScanFilter(const Expression &expr) {
	if (!expr.IsConsistent() || expr.CanThrow() || expr.HasParameter() || ExpressionBarrier::Contains(expr)) {
		return false;
	}
	// Optional wrappers keep their predicate in bind data rather than in the expression's children.
	auto optional_child = ExpressionFilter::GetOptionalFilterChild(expr);
	if (optional_child && !CanReorderScanFilter(*optional_child)) {
		return false;
	}
	bool can_reorder = true;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (can_reorder) {
			can_reorder = CanReorderScanFilter(child);
		}
	});
	return can_reorder;
}

static void CollectScanConjuncts(const Expression &expr, vector<reference<const Expression>> &conjuncts) {
	if (expr.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		conjuncts.push_back(expr);
		return;
	}
	for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
		CollectScanConjuncts(*child, conjuncts);
	}
}

static bool RewriteScanConjuncts(unique_ptr<Expression> &expr, reference_set_t<const Expression> &removed,
                                 reference_set_t<const Expression> &promoted) {
	// Consume references before destroying their source expressions.
	if (removed.erase(*expr)) {
		expr.reset();
		return true;
	}
	if (promoted.erase(*expr)) {
		expr = ExpressionFilter::GetOptionalFilterChild(*expr)->Copy();
		return true;
	}
	if (expr->GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		return false;
	}
	auto &children = expr->Cast<BoundConjunctionExpression>().GetChildrenMutable();
	bool changed = false;
	for (idx_t i = 0; i < children.size(); i++) {
		changed |= RewriteScanConjuncts(children[i], removed, promoted);
		if (!children[i]) {
			children.erase_at(i--);
		}
	}
	if (changed) {
		if (children.empty()) {
			expr.reset();
		} else if (children.size() == 1) {
			auto remaining = std::move(children[0]);
			expr = std::move(remaining);
		}
	}
	return changed;
}

FilterResult TableFilterSet::Finalize() {
	for (auto &entry : *this) {
		auto &filter = entry.Filter().Cast<ExpressionFilter>();
		if (filter.expr->GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			continue;
		}
		// Keep the original scan expressions and their order: a replacement form may not be supported by the scan.
		vector<reference<const Expression>> conjuncts;
		CollectScanConjuncts(*filter.expr, conjuncts);
		vector<reference<const Expression>> candidates;
		expression_set_t required;
		bool can_reorder = true;
		for (auto &conjunct : conjuncts) {
			if (!CanReorderScanFilter(conjunct.get())) {
				can_reorder = false;
				break;
			}
			if (!ExpressionFilter::GetOptionalFilterChild(conjunct.get()) &&
			    FilterCombiner::CanMatchExpression(conjunct.get())) {
				required.insert(conjunct.get());
			}
		}
		if (!can_reorder) {
			continue;
		}
		expression_set_t seen;
		reference_set_t<const Expression> removed;
		reference_set_t<const Expression> promoted;
		for (auto &conjunct : conjuncts) {
			auto &expr = conjunct.get();
			auto optional_child = ExpressionFilter::GetOptionalFilterChild(expr);
			if (ExpressionFilter::ContainsInternalTableFilterFunction(optional_child ? *optional_child : expr)) {
				continue;
			}
			// Expression equality can conflate constants that a function distinguishes, such as signed zeros.
			const bool can_match = FilterCombiner::CanMatchExpression(expr) &&
			                       (!optional_child || FilterCombiner::CanMatchExpression(*optional_child));
			if (can_match && optional_child && required.find(*optional_child) != required.end()) {
				removed.insert(expr);
				continue;
			}
			// Optional duplicates include the wrapper's configuration. Runtime state stays opaque.
			if (!can_match || seen.insert(expr).second) {
				candidates.push_back(expr);
			} else {
				removed.insert(expr);
			}
		}
		if (CombineScanComparisons(candidates, removed, promoted) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
		if (!removed.empty() || !promoted.empty()) {
			RewriteScanConjuncts(filter.expr, removed, promoted);
			D_ASSERT(filter.expr);
		}
	}
	return FilterResult::SUCCESS;
}

} // namespace duckdb
