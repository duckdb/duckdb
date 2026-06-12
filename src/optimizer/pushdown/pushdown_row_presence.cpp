#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_row_presence.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownRowPresence(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_ROW_PRESENCE);
	auto &row_presence = op->Cast<LogicalRowPresence>();
	// the row presence operator passes its child's bindings through unchanged:
	// filters can be pushed into the child unless they reference the presence column itself
	FilterPushdown child_pushdown(optimizer, convert_mark_joins);
	vector<unique_ptr<Expression>> remain_expressions;
	for (auto &filter : filters) {
		auto &f = *filter;
		bool can_push = true;
		for (auto &binding : f.bindings) {
			if (binding == row_presence.presence_index) {
				can_push = false;
				break;
			}
		}
		if (!can_push) {
			remain_expressions.push_back(std::move(f.filter));
		} else {
			if (child_pushdown.AddFilter(std::move(f.filter)) == FilterResult::UNSATISFIABLE) {
				// filter statically evaluates to false, strip tree
				return make_uniq<LogicalEmptyResult>(std::move(op));
			}
		}
	}
	child_pushdown.GenerateFilters();
	op->children[0] = child_pushdown.Rewrite(std::move(op->children[0]));
	if (op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
		// child returns an empty result: generate an empty result here too
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}
	return AddLogicalFilter(std::move(op), std::move(remain_expressions));
}

} // namespace duckdb
