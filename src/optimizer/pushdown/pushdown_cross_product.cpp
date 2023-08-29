#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb
{
using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op)
{
	D_ASSERT(op->logical_type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	vector<unique_ptr<Expression>> join_expressions;
	unordered_set<idx_t> left_bindings, right_bindings;
	if (!filters.empty())
	{
		// check to see into which side we should push the filters
		// first get the LHS and RHS bindings
		auto left = unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[0]));
		LogicalJoin::GetTableReferences(*left, left_bindings);
		op->children[0] = std::move(left);
		auto right = unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[1]));
		LogicalJoin::GetTableReferences(*right, right_bindings);
		op->children[1] = std::move(right);
		// now check the set of filters
		for (auto &f : filters)
		{
			auto side = JoinSide::GetJoinSide(f->bindings, left_bindings, right_bindings);
			if (side == JoinSide::LEFT)
			{
				// bindings match left side: push into left
				left_pushdown.filters.push_back(std::move(f));
			}
			else if (side == JoinSide::RIGHT)
			{
				// bindings match right side: push into right
				right_pushdown.filters.push_back(std::move(f));
			}
			else
			{
				D_ASSERT(side == JoinSide::BOTH || side == JoinSide::NONE);
				// bindings match both: turn into join condition
				join_expressions.push_back(std::move(f->filter));
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[0])));
	op->children[1] = right_pushdown.Rewrite(unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[1])));
	if (!join_expressions.empty())
	{
		// join conditions found: turn into inner join
		// extract join conditions
		vector<JoinCondition> conditions;
		vector<unique_ptr<Expression>> arbitrary_expressions;
		auto join_type = JoinType::INNER;
		unique_ptr<LogicalOperator> left_child = unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[0]));
		unique_ptr<LogicalOperator> right_child = unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[1]));
		LogicalComparisonJoin::ExtractJoinConditions(join_type, left_child, right_child, left_bindings, right_bindings, join_expressions, conditions, arbitrary_expressions);
		op->children[0] = std::move(left_child);
		op->children[1] = std::move(right_child);
		// create the join from the join conditions
		return LogicalComparisonJoin::CreateJoin(JoinType::INNER, JoinRefType::REGULAR, unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[0])), unique_ptr_cast<Operator, LogicalOperator>(std::move(op->children[1])), std::move(conditions), std::move(arbitrary_expressions));
	}
	else
	{
		// no join conditions found: keep as cross product
		return op;
	}
}
} // namespace duckdb