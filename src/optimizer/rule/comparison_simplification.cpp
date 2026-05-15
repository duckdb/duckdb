#include "duckdb/planner/expression/list.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

static unique_ptr<Expression> DateTimestampComparisonIsInvertible(BoundFunctionExpression &expr,
                                                                  BoundCastExpression &cast_expression,
                                                                  const Value &constant_value, Value &cast_constant,
                                                                  bool column_ref_left) {
	if (Timestamp::GetTime(constant_value.GetValue<timestamp_t>()) == dtime_t(0)) {
		return nullptr; // it's midnight: no replacement needed
	}
	auto op = expr.GetExpressionType();
	bool pred_neq = op == ExpressionType::COMPARE_NOTEQUAL || op == ExpressionType::COMPARE_DISTINCT_FROM;
	if (op == ExpressionType::COMPARE_EQUAL || op == ExpressionType::COMPARE_NOTEQUAL ||
	    op == ExpressionType::COMPARE_DISTINCT_FROM || op == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		// Non-midnight TIMESTAMPs cannot equal DATE values.
		if (op == ExpressionType::COMPARE_DISTINCT_FROM || op == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return make_uniq<BoundConstantExpression>(Value::BOOLEAN(pred_neq));
		}
		// Equality keeps three-valued NULL semantics; DISTINCT FROM is always two-valued.
		return ExpressionRewriter::ConstantOrNull(std::move(cast_expression.child), Value::BOOLEAN(pred_neq));
	}

	bool if_left_plus = op == ExpressionType::COMPARE_LESSTHAN || op == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	// d <  T   -> d <  DATE '2024-06-16'  -- needs +1
	// d >= T   -> d >= DATE '2024-06-16'  -- needs +1
	// d <= T   -> d <= DATE '2024-06-15'  -- no +1
	// d >  T   -> d >  DATE '2024-06-15'  -- no +1
	if (!if_left_plus && op != ExpressionType::COMPARE_LESSTHANOREQUALTO && op != ExpressionType::COMPARE_GREATERTHAN) {
		return nullptr;
	}
	if (column_ref_left == if_left_plus) { // if ref_left & left_plus or "ref_right & if_right_plus"
		cast_constant = Value::DATE(date_t(cast_constant.GetValue<date_t>().days + 1));
	}
	return nullptr;
}

static bool ConstantCastIsInvertible(BoundFunctionExpression &expr, BoundCastExpression &cast_expression,
                                     const Value &constant_value, Value &cast_constant, const LogicalType &target_type,
                                     bool column_ref_left, unique_ptr<Expression> &replacement) {
	if (cast_constant.IsNull() || BoundCastExpression::CastIsInvertible(cast_expression.GetReturnType(), target_type)) {
		return true;
	}
	if (target_type.id() != LogicalTypeId::DATE || cast_expression.GetReturnType().id() != LogicalTypeId::TIMESTAMP) {
		return false;
	}
	replacement =
	    DateTimestampComparisonIsInvertible(expr, cast_expression, constant_value, cast_constant, column_ref_left);
	return true;
}

ComparisonSimplificationRule::ComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_uniq<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

unique_ptr<Expression> ComparisonSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                           bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &constant_expr = bindings[1].get();
	auto &left = BoundComparisonExpression::LeftMutable(expr);
	auto &right = BoundComparisonExpression::RightMutable(expr);
	bool column_ref_left = !RefersToSameObject(*left, constant_expr);
	auto &column_ref_expr = column_ref_left ? *left : *right;
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	D_ASSERT(constant_expr.IsFoldable());
	Value constant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), constant_expr, constant_value)) {
		return nullptr;
	}
	if (constant_value.IsNull() && !(expr.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
	                                 expr.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM)) {
		// comparison with constant NULL, return NULL
		return make_uniq<BoundConstantExpression>(Value(LogicalType::BOOLEAN));
	}
	if (column_ref_expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		//! Here we check if we can apply the expression on the constant side
		//! We can do this if the cast itself is invertible and casting the constant is
		//! invertible in practice.
		auto &cast_expression = column_ref_expr.Cast<BoundCastExpression>();
		auto target_type = cast_expression.source_type();
		if (!BoundCastExpression::CastIsInvertible(target_type, cast_expression.GetReturnType())) {
			return nullptr;
		}

		// Can we cast the constant at all?
		string error_message;
		Value cast_constant;
		auto new_constant =
		    constant_value.TryCastAs(rewriter.context, target_type, cast_constant, &error_message, true);
		if (!new_constant) {
			return nullptr;
		}

		// Is the constant cast invertible?
		unique_ptr<Expression> replacement;
		if (!ConstantCastIsInvertible(expr, cast_expression, constant_value, cast_constant, target_type,
		                              column_ref_left, replacement)) {
			return nullptr;
		}
		if (replacement) {
			return replacement;
		}

		//! We can cast, now we change our column_ref_expression from an operator cast to a column reference
		auto child_expression = std::move(cast_expression.child);
		auto new_constant_expr = make_uniq<BoundConstantExpression>(cast_constant);
		if (column_ref_left) {
			left = std::move(child_expression);
			right = std::move(new_constant_expr);
		} else {
			left = std::move(new_constant_expr);
			right = std::move(child_expression);
		}
		changes_made = true;
	}
	return nullptr;
}

} // namespace duckdb
