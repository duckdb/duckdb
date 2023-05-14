#include "duckdb/planner/expression/list.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

ComparisonSimplificationRule::ComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_uniq<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

unique_ptr<Expression> ComparisonSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                           bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundComparisonExpression>();
	auto &constant_expr = bindings[1].get();
	bool column_ref_left = expr.left.get() != &constant_expr;
	auto column_ref_expr = !column_ref_left ? expr.right.get() : expr.left.get();
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	D_ASSERT(constant_expr.IsFoldable());
	Value constant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), constant_expr, constant_value)) {
		return nullptr;
	}
	if (constant_value.IsNull() && !(expr.type == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
	                                 expr.type == ExpressionType::COMPARE_DISTINCT_FROM)) {
		// comparison with constant NULL, return NULL
		return make_uniq<BoundConstantExpression>(Value(LogicalType::BOOLEAN));
	}
	if (column_ref_expr->expression_class == ExpressionClass::BOUND_CAST) {
		//! Here we check if we can apply the expression on the constant side
		//! We can do this if the cast itself is invertible and casting the constant is
		//! invertible in practice.
		auto &cast_expression = column_ref_expr->Cast<BoundCastExpression>();
		auto target_type = cast_expression.source_type();
		if (!BoundCastExpression::CastIsInvertible(target_type, cast_expression.return_type)) {
			return nullptr;
		}

		// Can we cast the constant at all?
		string error_message;
		Value cast_constant;
		auto new_constant = constant_value.DefaultTryCastAs(target_type, cast_constant, &error_message, true);
		if (!new_constant) {
			return nullptr;
		}

		// Is the constant cast invertible?
		if (!cast_constant.IsNull() &&
		    !BoundCastExpression::CastIsInvertible(cast_expression.return_type, target_type)) {
			// Is it actually invertible?
			Value uncast_constant;
			if (!cast_constant.DefaultTryCastAs(constant_value.type(), uncast_constant, &error_message, true) ||
			    uncast_constant != constant_value) {
				return nullptr;
			}
		}

		//! We can cast, now we change our column_ref_expression from an operator cast to a column reference
		auto child_expression = std::move(cast_expression.child);
		auto new_constant_expr = make_uniq<BoundConstantExpression>(cast_constant);
		if (column_ref_left) {
			expr.left = std::move(child_expression);
			expr.right = std::move(new_constant_expr);
		} else {
			expr.left = std::move(new_constant_expr);
			expr.right = std::move(child_expression);
		}
	}
	return nullptr;
}

} // namespace duckdb
