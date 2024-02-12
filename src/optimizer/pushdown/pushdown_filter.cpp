#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	if (!filter.projection_map.empty()) {
		return FinishPushdown(std::move(op));
	}
	// filter: gather the filters and remove the filter from the set of operations
	for (auto &expression : filter.expressions) {
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}
	GenerateFilters();
	// we don't want to loose the projection map of the filter.
	// so we rewrite the child with the filters, then keep the filter
	// operator so we can maintain the projection map.
	auto child = Rewrite(std::move(filter.children[0]));
	if (filter.projection_map.empty()) {
		return child;
	}
	// if there is a projection map, you need to keep that information
	filter.expressions.clear();
	op->children[0] = std::move(child);
	return op;
}

} // namespace duckdb
