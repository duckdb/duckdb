#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownSecureView(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_SECURE_VIEW);
	// filters can be pushed into a secure view, but only if evaluating them on rows that the view does not emit
	// cannot be observed from the outside. Expressions that can throw an error or have side effects are wrapped in
	// a barrier: those are never pushed past an operator inside the view that removes rows, and are always
	// evaluated after the filters they end up next to.
	//
	// freeze the cardinality estimate of the view before anything is pushed into it - otherwise the estimate of the
	// boundary node reports what the statistics of the view contents say about the caller's predicate
	if (!filters.empty() && !op->has_estimated_cardinality) {
		op->SetEstimatedCardinality(op->children[0]->EstimateCardinality(optimizer.GetContext()));
	}

	auto &secure_view = op->Cast<LogicalSecureView>();
	FilterPushdown child_pushdown(optimizer, convert_mark_joins, projection_mode);
	for (auto &f : filters) {
		auto expr = std::move(f->filter);
		// the operators inside the view are never shown - report the filter as part of the boundary node instead
		secure_view.pushed_filters.push_back(expr->ToString());
		if (ExpressionBarrier::Required(*expr) && !ExpressionBarrier::Contains(*expr)) {
			expr = ExpressionBarrier::Wrap(std::move(expr));
		}
		if (child_pushdown.AddFilter(std::move(expr)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}
	filters.clear();
	child_pushdown.GenerateFilters();

	// note that the boundary node is always kept in place, even if the child collapses into an empty result - the
	// plan outside the view must not depend on anything the optimizer learns about the contents of the view
	op->children[0] = child_pushdown.Rewrite(std::move(op->children[0]));
	return op;
}

} // namespace duckdb
