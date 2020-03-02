#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::CROSS_PRODUCT);
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	vector<unique_ptr<Expression>> join_conditions;
	unordered_set<idx_t> left_bindings, right_bindings;
	if (filters.size() > 0) {
		// check to see into which side we should push the filters
		// first get the LHS and RHS bindings
		LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
		LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
		// now check the set of filters
		for (auto &f : filters) {
			auto side = JoinSide::GetJoinSide(f->bindings, left_bindings, right_bindings);
			if (side == JoinSide::LEFT) {
				// bindings match left side: push into left
				left_pushdown.filters.push_back(move(f));
			} else if (side == JoinSide::RIGHT) {
				// bindings match right side: push into right
				right_pushdown.filters.push_back(move(f));
			} else {
				assert(side == JoinSide::BOTH);
				// bindings match both: turn into join condition
				join_conditions.push_back(move(f->filter));
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));

	if (join_conditions.size() > 0) {
		// join conditions found: turn into inner join
		return LogicalComparisonJoin::CreateJoin(JoinType::INNER, move(op->children[0]), move(op->children[1]),
		                                         left_bindings, right_bindings, join_conditions);
	} else {
		// no join conditions found: keep as cross product
		return op;
	}
}
