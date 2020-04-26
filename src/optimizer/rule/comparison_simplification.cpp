#include "duckdb/planner/expression/list.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

using namespace duckdb;
using namespace std;

ComparisonSimplificationRule::ComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_unique<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_unique<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = move(op);
}

unique_ptr<Expression> ComparisonSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made) {
	assert(bindings[0]->expression_class == ExpressionClass::BOUND_COMPARISON);
	auto expr = (BoundComparisonExpression *)bindings[0];
	auto constant_expr = bindings[1];
	bool column_ref_left = expr->left.get() != constant_expr;
	auto column_ref_expr = !column_ref_left ? expr->right.get() : expr->left.get();
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	assert(constant_expr->IsFoldable());
	auto constant_value = ExpressionExecutor::EvaluateScalar(*constant_expr);
	if (constant_value.is_null) {
		// comparison with constant NULL, return NULL
		return make_unique<BoundConstantExpression>(Value(TypeId::BOOL));
	}
	if (column_ref_expr->expression_class == ExpressionClass::BOUND_CAST &&
	    constant_expr->expression_class == ExpressionClass::BOUND_CONSTANT) {
		//! Here we check if we can apply the expression on the constant side
		auto cast_expression = (BoundCastExpression *)column_ref_expr;
		if (!BoundCastExpression::CastIsInvertible(cast_expression->source_type, cast_expression->target_type)) {
			return nullptr;
		}
		auto bound_const_expr = (BoundConstantExpression *)constant_expr;
		auto new_constant =
		    bound_const_expr->value.TryCastAs(cast_expression->target_type.id, cast_expression->source_type.id);
		if (new_constant) {
			auto child_expression = move(cast_expression->child);
			constant_expr->return_type = bound_const_expr->value.type;
			//! We can cast, now we change our column_ref_expression from an operator cast to a column reference
			if (column_ref_left) {
				expr->left = move(child_expression);
			} else {
				expr->right = move(child_expression);
			}
		}
	}
	return nullptr;
}
