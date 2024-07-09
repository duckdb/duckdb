#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->children.size() > 1);
	FilterPushdown left_pushdown(optimizer, convert_mark_joins), right_pushdown(optimizer, convert_mark_joins);
	vector<unique_ptr<Expression>> join_expressions;
	auto join_ref_type = JoinRefType::REGULAR;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		break;
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		join_ref_type = JoinRefType::ASOF;
		break;
	default:
		throw InternalException("Unsupported join type for cross product push down");
	}
	unordered_set<idx_t> left_bindings, right_bindings;
	if (!filters.empty()) {
		// check to see into which side we should push the filters
		// first get the LHS and RHS bindings
		LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
		LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
		// now check the set of filters
		for (auto &f : filters) {
			auto side = JoinSide::GetJoinSide(f->bindings, left_bindings, right_bindings);
			if (side == JoinSide::LEFT) {
				// bindings match left side: push into left
				left_pushdown.filters.push_back(std::move(f));
			} else if (side == JoinSide::RIGHT) {
				// bindings match right side: push into right
				if (join_ref_type == JoinRefType::ASOF) {
					// AsOf is really a table lookup, so we don't push filters
					// down into the lookup (right) table
					join_expressions.push_back(std::move(f->filter));
				} else {
					right_pushdown.filters.push_back(std::move(f));
				}
			} else {
				D_ASSERT(side == JoinSide::BOTH || side == JoinSide::NONE);
				// bindings match both: turn into join condition
				join_expressions.push_back(std::move(f->filter));
			}
		}
	}

	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));

	if (!join_expressions.empty()) {
		// join conditions found: turn into inner join
		// extract join conditions
		vector<JoinCondition> conditions;
		vector<unique_ptr<Expression>> arbitrary_expressions;
		const auto join_type = JoinType::INNER;
		LogicalComparisonJoin::ExtractJoinConditions(GetContext(), join_type, join_ref_type, op->children[0],
		                                             op->children[1], left_bindings, right_bindings, join_expressions,
		                                             conditions, arbitrary_expressions);
		// create the join from the join conditions
		return LogicalComparisonJoin::CreateJoin(GetContext(), join_type, join_ref_type, std::move(op->children[0]),
		                                         std::move(op->children[1]), std::move(conditions),
		                                         std::move(arbitrary_expressions));
	} else {
		// no join conditions found: keep as cross product
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
		return op;
	}
}

} // namespace duckdb
