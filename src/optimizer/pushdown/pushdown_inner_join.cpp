#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                              unordered_set<idx_t> &left_bindings,
                                                              unordered_set<idx_t> &right_bindings) {
	auto &join = (LogicalJoin &)*op;
	assert(join.join_type == JoinType::INNER);
	assert(op->type != LogicalOperatorType::DELIM_JOIN);
	// inner join: gather all the conditions of the inner join and add to the filter list
	if (op->type == LogicalOperatorType::ANY_JOIN) {
		auto &any_join = (LogicalAnyJoin &)join;
		// any join: only one filter to add
		if (AddFilter(move(any_join.condition)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(move(op));
		}
	} else {
		// comparison join
		assert(op->type == LogicalOperatorType::COMPARISON_JOIN);
		auto &comp_join = (LogicalComparisonJoin &)join;
		// turn the conditions into filters
		for (idx_t i = 0; i < comp_join.conditions.size(); i++) {
			auto condition = JoinCondition::CreateExpression(move(comp_join.conditions[i]));
			if (AddFilter(move(condition)) == FilterResult::UNSATISFIABLE) {
				// filter statically evaluates to false, strip tree
				return make_unique<LogicalEmptyResult>(move(op));
			}
		}
	}
	GenerateFilters();

	// turn the inner join into a cross product
	auto cross_product = make_unique<LogicalCrossProduct>();
	cross_product->children.push_back(move(op->children[0]));
	cross_product->children.push_back(move(op->children[1]));
	// then push down cross product
	return PushdownCrossProduct(move(cross_product));
}
