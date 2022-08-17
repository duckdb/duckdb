#include "duckdb/planner/expression/list.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

ComparisonSimplificationRule::ComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_unique<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_unique<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = move(op);
}

unique_ptr<Expression> ComparisonSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made, bool is_root) {
	D_ASSERT(bindings[0]->expression_class == ExpressionClass::BOUND_COMPARISON);
	auto expr = (BoundComparisonExpression *)bindings[0];
	auto constant_expr = bindings[1];
	bool column_ref_left = expr->left.get() != constant_expr;
	auto column_ref_expr = !column_ref_left ? expr->right.get() : expr->left.get();
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	D_ASSERT(constant_expr->IsFoldable());
	Value constant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(*constant_expr, constant_value)) {
		return nullptr;
	}
	if (constant_value.IsNull() && !(expr->type == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
	                                 expr->type == ExpressionType::COMPARE_DISTINCT_FROM)) {
		// comparison with constant NULL, return NULL
		return make_unique<BoundConstantExpression>(Value(LogicalType::BOOLEAN));
	}
	if (column_ref_expr->expression_class == ExpressionClass::BOUND_CAST) {
		//! Here we check if we can apply the expression on the constant side
		auto cast_expression = (BoundCastExpression *)column_ref_expr;
		auto target_type = cast_expression->source_type();
		if (!BoundCastExpression::CastIsInvertible(target_type, cast_expression->return_type) ||
		    !BoundCastExpression::CastIsInvertible(cast_expression->return_type, target_type)) {
			return nullptr;
		}
		auto new_constant = constant_value.TryCastAs(target_type, true);
		if (new_constant) {
			auto child_expression = move(cast_expression->child);
			auto new_constant_expr = make_unique<BoundConstantExpression>(constant_value);
			//! We can cast, now we change our column_ref_expression from an operator cast to a column reference
			if (column_ref_left) {
				expr->left = move(child_expression);
				expr->right = move(new_constant_expr);
			} else {
				expr->left = move(new_constant_expr);
				expr->right = move(child_expression);
			}
		}
	}
	return nullptr;
}

} // namespace duckdb
