#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	if (filter.HasProjectionMap()) {
		if (!filter.expressions.empty()) {
			return FinishPushdown(std::move(op));
		}
		vector<unique_ptr<Expression>> remain_expressions;
		for (idx_t i = 0; i < filters.size(); i++) {
			if (!filters[i]->filter->CanThrow() && !filters[i]->filter->IsVolatile()) {
				continue;
			}
			remain_expressions.push_back(std::move(filters[i]->filter));
			filters.erase_at(i--);
		}
		// Preserve the output layout and the evaluation boundary of unsafe predicates.
		RewriteChild(*op, 0);
		return AddLogicalFilter(std::move(op), std::move(remain_expressions));
	}
	// filter: gather the filters and remove the filter from the set of operations
	for (auto &expression : filter.expressions) {
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}
	GenerateFilters();
	return Rewrite(std::move(filter.children[0]));
}

} // namespace duckdb
