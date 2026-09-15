#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownDistinct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_DISTINCT);
	auto &distinct = op->Cast<LogicalDistinct>();
	if (!distinct.order_by) {
		// A predicate may distinguish values that DISTINCT considers equal under a collation, changing which
		// representative survives. Keep those filters above DISTINCT and push the remaining filters normally.
		FilterPushdown pushdown(optimizer, convert_mark_joins, projection_mode);
		vector<unique_ptr<Filter>> remaining_filters;
		for (auto &filter : filters) {
			if (FilterUsesCollation(*filter)) {
				remaining_filters.push_back(std::move(filter));
			} else {
				pushdown.filters.push_back(std::move(filter));
			}
		}
		filters = std::move(remaining_filters);
		op->children[0] = pushdown.Rewrite(std::move(op->children[0]));
		return PushFinalFilters(std::move(op));
	}
	// no pushdown through DISTINCT ON (yet?)
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
