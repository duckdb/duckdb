#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	if (filter.children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
	    filter.children[0]->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = filter.children[0]->Cast<LogicalJoin>();
		if (join.join_type == JoinType::LEFT) {
			// Filters above LEFT LATERAL joins can reference NULL-extended RHS outputs.
			// Keep any filter that depends on the RHS above the join to preserve outer join semantics.
			unordered_set<TableIndex> left_bindings, right_bindings;
			LogicalJoin::GetTableReferences(*join.children[0], left_bindings);
			LogicalJoin::GetTableReferences(*join.children[1], right_bindings);
			vector<unique_ptr<Expression>> remain_expressions;
			for (auto &expression : filter.expressions) {
				auto side = JoinSide::GetJoinSide(*expression, left_bindings, right_bindings);
				if (side != JoinSide::LEFT) {
					remain_expressions.push_back(std::move(expression));
					continue;
				}
				if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
					return make_uniq<LogicalEmptyResult>(std::move(op));
				}
			}
			GenerateFilters();
			auto child = Rewrite(std::move(filter.children[0]));
			return AddLogicalFilter(std::move(child), std::move(remain_expressions));
		}
	}
	if (filter.HasProjectionMap()) {
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
	return Rewrite(std::move(filter.children[0]));
}

} // namespace duckdb
