#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
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
		// We can only push down right side AsOf expressions
		// that do not reference the inequality column
		vector<unique_ptr<Filter>> asof_filters;
		optional_ptr<Expression> asof_compare;
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
					asof_filters.push_back(std::move(f));
				} else {
					right_pushdown.filters.push_back(std::move(f));
				}
			} else {
				D_ASSERT(side == JoinSide::BOTH || side == JoinSide::NONE);
				// Record the inequality binding for AsOf
				if (join_ref_type == JoinRefType::ASOF && ExpressionType::COMPARE_LESSTHAN <= f->filter->type &&
				    f->filter->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
					asof_compare = f->filter->Cast<BoundComparisonExpression>().right.get();
				}
				// bindings match both: turn into join condition
				join_expressions.push_back(std::move(f->filter));
			}
		}
		//	Now that we know what the right side AsOf inequality expression is,
		//	we can push down any predicates that don't refer to it
		for (auto &f : asof_filters) {
			bool referenced = false;
			ExpressionIterator::EnumerateExpression(f->filter, [&](const Expression &child) {
				if (child.Equals(*asof_compare)) {
					referenced = true;
				}
			});
			if (referenced) {
				join_expressions.push_back(std::move(f->filter));
			} else {
				right_pushdown.filters.push_back(std::move(f));
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
		auto new_op = LogicalComparisonJoin::CreateJoin(GetContext(), join_type, join_ref_type,
		                                                std::move(op->children[0]), std::move(op->children[1]),
		                                                std::move(conditions), std::move(arbitrary_expressions));

		// possible cases are: AnyJoin, ComparisonJoin, or Filter + ComparisonJoin
		if (op->has_estimated_cardinality) {
			// set the estimated cardinality of the new operator
			new_op->SetEstimatedCardinality(op->estimated_cardinality);
			if (new_op->type == LogicalOperatorType::LOGICAL_FILTER) {
				// if the new operators are Filter + ComparisonJoin, also set the estimated cardinality for the join
				D_ASSERT(new_op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
				new_op->children[0]->SetEstimatedCardinality(op->estimated_cardinality);
			}
		}
		return new_op;
	} else {
		// no join conditions found: keep as cross product
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
		return op;
	}
}

} // namespace duckdb
