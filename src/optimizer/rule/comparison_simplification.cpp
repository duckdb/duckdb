#include "duckdb/planner/expression/list.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

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
		if (!cast_constant.IsNull() &&
		    !BoundCastExpression::CastIsInvertible(cast_expression.GetReturnType(), target_type)) {
			// DATE→TIMESTAMP is lossless: every DATE maps to a unique midnight TIMESTAMP.
			// For comparisons with a TIMESTAMP constant, we can rewrite using the floor (cast to DATE),
			// adjusting by +1 day for operators where the column-as-midnight value is strictly excluded
			// by a non-midnight constant.
			if (target_type.id() != LogicalTypeId::DATE ||
			    cast_expression.GetReturnType().id() != LogicalTypeId::TIMESTAMP) {
				return nullptr;
			}
			if (Timestamp::GetTime(constant_value.GetValue<timestamp_t>()) != dtime_t(0)) {
				auto op = expr.GetExpressionType();
				// =/!= against a non-midnight TIMESTAMP can never match a DATE column (which is always
				// midnight when cast to TIMESTAMP) — replace with constant_or_null(col, false/true).
				if (op == ExpressionType::COMPARE_EQUAL || op == ExpressionType::COMPARE_NOTEQUAL) {
					return ExpressionRewriter::ConstantOrNull(std::move(cast_expression.child),
					                                          Value::BOOLEAN(op == ExpressionType::COMPARE_NOTEQUAL));
				}
				bool needs_plus_one_day = column_ref_left ? (op == ExpressionType::COMPARE_LESSTHAN ||
				                                             op == ExpressionType::COMPARE_GREATERTHANOREQUALTO)
				                                          : (op == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
				                                             op == ExpressionType::COMPARE_GREATERTHAN);
				if (op != ExpressionType::COMPARE_LESSTHAN && op != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
				    op != ExpressionType::COMPARE_GREATERTHAN && op != ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
					return nullptr;
				}
				if (needs_plus_one_day) {
					cast_constant = Value::DATE(date_t(cast_constant.GetValue<date_t>().days + 1));
				}
			}
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
