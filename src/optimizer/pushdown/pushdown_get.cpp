#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {
unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = op->Cast<LogicalGet>();

	if (get.function.pushdown_complex_filter || get.function.filter_pushdown) {
		// this scan supports some form of filter push-down
		// check if there are any parameters
		// if there are, invalidate them to force a re-bind on execution
		for (auto &filter : filters) {
			if (filter->filter->HasParameter()) {
				// there is a parameter in the filters! invalidate it
				BoundParameterExpression::InvalidateRecursive(*filter->filter);
			}
		}
	}
	if (get.function.pushdown_complex_filter) {
		// for the remaining filters, check if we can push any of them into the scan as well
		vector<unique_ptr<Expression>> expressions;
		expressions.reserve(filters.size());
		for (auto &filter : filters) {
			expressions.push_back(std::move(filter->filter));
		}
		filters.clear();

		get.function.pushdown_complex_filter(optimizer.context, get, get.bind_data.get(), expressions);

		if (expressions.empty()) {
			return op;
		}
		// re-generate the filters
		for (auto &expr : expressions) {
			auto f = make_uniq<Filter>();
			f->filter = std::move(expr);
			f->ExtractBindings();
			filters.push_back(std::move(f));
		}
	}

	if (!get.table_filters.filters.empty() || !get.function.filter_pushdown) {
		// the table function does not support filter pushdown: push a LogicalFilter on top
		return FinishPushdown(std::move(op));
	}
	if (PushFilters() == FilterResult::UNSATISFIABLE) {
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	auto &column_ids = get.GetColumnIds();
	//! We generate the table filters that will be executed during the table scan
	vector<FilterPushdownResult> pushdown_results;
	get.table_filters = combiner.GenerateTableScanFilters(column_ids, pushdown_results);

	GenerateFilters();

	for (idx_t i = pushdown_results.size(); i < filters.size(); ++i) {
		// any generated filters have not been pushed down yet
		pushdown_results.push_back(FilterPushdownResult::NO_PUSHDOWN);
	}
	// for any filters we did not manage to push into specialized table filters - try to push them as a generic
	// expression
	for (idx_t i = 0; i < filters.size(); ++i) {
		// get the previous pushdown result
		auto pushdown_result = pushdown_results[i];
		if (pushdown_result != FilterPushdownResult::NO_PUSHDOWN) {
			// this has already been (partially) pushed down - skip
			continue;
		}
		auto &expr = *filters[i]->filter;
		if (expr.IsVolatile()) {
			continue;
		}
		// Allow pushing down filters that can throw only if there is a single expression
		// For now, do not push down single expressions with IN either. Later we can change InClauseRewriter to handle
		// this case
		if (expr.CanThrow() && (expr.type == ExpressionType::COMPARE_IN || filters.size() > 1)) {
			continue;
		}
		pushdown_result = combiner.TryPushdownGenericExpression(get, expr);
		if (pushdown_result == FilterPushdownResult::PUSHED_DOWN_FULLY) {
			filters.erase_at(i);
			pushdown_results.erase_at(i);
			i--;
		}
	}

	//! Now we try to pushdown the remaining filters to perform zonemap checking
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
