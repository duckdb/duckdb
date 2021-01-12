#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;


unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	vector<unique_ptr<Expression>> join_conditions;
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
				left_pushdown.filters.push_back(move(f));
			} else if (side == JoinSide::RIGHT) {
				// bindings match right side: push into right
				right_pushdown.filters.push_back(move(f));
			} else {
				D_ASSERT(side == JoinSide::BOTH);
				// bindings match both: turn into join condition
				join_conditions.push_back(move(f->filter));
			}
		}
	}

	//! Check if columns in join condition have filters and push them down to both tables.
	vector<unique_ptr<Filter>> filters_add_right;
	for (auto& join_cond: join_conditions){
		if (join_cond->type == ExpressionType::COMPARE_EQUAL){
			auto &comp_exp = (BoundComparisonExpression &)*join_cond;
			if (comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
			    comp_exp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF){
				auto &col_left = (ColumnRefExpression &)*comp_exp.left;
				auto &col_right = (ColumnRefExpression &)*comp_exp.right;
				for (auto& left_filters: left_pushdown.filters){
					if (left_filters->filter->expression_class == ExpressionClass::BOUND_COMPARISON){
						auto &f_comp_exp = (BoundComparisonExpression &)*left_filters->filter;
						if (f_comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
						    f_comp_exp.left->expression_class == ExpressionClass::CONSTANT){
							auto &f_col_left = (ColumnRefExpression &)*f_comp_exp.left;
							if (f_col_left == col_left){
								auto pushdown_expr = left_filters->filter->Copy();
								auto &push_comp_exp = (BoundComparisonExpression &)*pushdown_expr;
								push_comp_exp.left = comp_exp.right->Copy();
								unique_ptr<Filter> pushdown_filter = make_unique<Filter>(move(pushdown_expr));
								filters_add_right.push_back(move(pushdown_filter));
							}
						}
					}
				}
				for (auto& right_filters: right_pushdown.filters){
					if (right_filters->filter->expression_class == ExpressionClass::BOUND_COMPARISON){
						auto &f_comp_exp = (BoundComparisonExpression &)*right_filters->filter;
						if (f_comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
						    f_comp_exp.left->expression_class == ExpressionClass::CONSTANT){
							auto &f_col_right = (ColumnRefExpression &)*f_comp_exp.right;
							if (f_col_right == col_right){
								auto pushdown_expr = right_filters->filter->Copy();
								auto &push_comp_exp = (BoundComparisonExpression &)*pushdown_expr;
								push_comp_exp.left = comp_exp.left->Copy();
								unique_ptr<Filter> pushdown_filter = make_unique<Filter>(move(pushdown_expr));
								left_pushdown.filters.push_back(move(pushdown_filter));
							}
						}
					}
				}
				for (auto& r_filters: filters_add_right){
					right_pushdown.filters.push_back(move(r_filters));
				}
			}

		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));

	if (!join_conditions.empty()) {
		// join conditions found: turn into inner join
		return LogicalComparisonJoin::CreateJoin(JoinType::INNER, move(op->children[0]), move(op->children[1]),
		                                         left_bindings, right_bindings, join_conditions);
	} else {
		// no join conditions found: keep as cross product
		return op;
	}
}

} // namespace duckdb
