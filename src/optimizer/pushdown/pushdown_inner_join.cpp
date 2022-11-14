#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                              unordered_set<idx_t> &left_bindings,
                                                              unordered_set<idx_t> &right_bindings) {
	auto &join = (LogicalJoin &)*op;
	D_ASSERT(join.join_type == JoinType::INNER);
	D_ASSERT(op->type != LogicalOperatorType::LOGICAL_DELIM_JOIN);
	// inner join: gather all the conditions of the inner join and add to the filter list
	if (op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto &any_join = (LogicalAnyJoin &)join;
		// any join: only one filter to add
		if (AddFilter(move(any_join.condition)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(move(op));
		}
	} else {
		// comparison join
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
		auto &comp_join = (LogicalComparisonJoin &)join;
		// turn the conditions into filters
		for (auto &i : comp_join.conditions) {
			auto condition = JoinCondition::CreateExpression(move(i));
			if (AddFilter(move(condition)) == FilterResult::UNSATISFIABLE) {
				// filter statically evaluates to false, strip tree
				return make_unique<LogicalEmptyResult>(move(op));
			}
		}
	}
	GenerateFilters();

	// turn the inner join into a cross product
	auto cross_product = make_unique<LogicalCrossProduct>(move(op->children[0]), move(op->children[1]));
	// then push down cross product
	return PushdownCrossProduct(move(cross_product));
}

} // namespace duckdb
