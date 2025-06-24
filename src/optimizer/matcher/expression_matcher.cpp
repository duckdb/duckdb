#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/planner/expression/list.hpp"

namespace duckdb {

bool ExpressionMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	if (type && !type->Match(expr.return_type)) {
		return false;
	}
	if (expr_type && !expr_type->Match(expr.GetExpressionType())) {
		return false;
	}
	if (expr_class != ExpressionClass::INVALID && expr_class != expr.GetExpressionClass()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool ExpressionEqualityMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	if (!expr.Equals(expression)) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool CaseExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	return true;
}

bool ComparisonExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundComparisonExpression>();
	vector<reference<Expression>> expressions;
	expressions.push_back(*expr.left);
	expressions.push_back(*expr.right);
	return SetMatcher::Match(matchers, expressions, bindings, policy);
}

bool CastExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	if (!matcher) {
		return true;
	}
	auto &expr = expr_p.Cast<BoundCastExpression>();
	return matcher->Match(*expr.child, bindings);
}

bool InClauseExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundOperatorExpression>();
	if (expr.GetExpressionType() != ExpressionType::COMPARE_IN ||
	    expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
		return false;
	}
	return SetMatcher::Match(matchers, expr.children, bindings, policy);
}

bool ConjunctionExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundConjunctionExpression>();
	if (!SetMatcher::Match(matchers, expr.children, bindings, policy)) {
		return false;
	}
	return true;
}

bool FunctionExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundFunctionExpression>();
	if (!FunctionMatcher::Match(function, expr.function.name)) {
		return false;
	}
	if (!SetMatcher::Match(matchers, expr.children, bindings, policy)) {
		return false;
	}
	return true;
}

bool AggregateExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundAggregateExpression>();
	if (!FunctionMatcher::Match(function, expr.function.name)) {
		return false;
	}
	// we should create matchers for these in the future
	if (expr.filter || expr.order_bys || expr.aggr_type != AggregateType::NON_DISTINCT) {
		return false;
	}
	if (!SetMatcher::Match(matchers, expr.children, bindings, policy)) {
		return false;
	}
	return true;
}

bool FoldableConstantMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	// we match on ANY expression that is a scalar expression
	if (!expr.IsFoldable()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool StableExpressionMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	// we match on ANY expression that is a stable expression
	if (expr.IsVolatile()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

} // namespace duckdb
