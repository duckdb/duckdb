#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/planner/expression/list.hpp"

namespace duckdb {

bool ExpressionMatcher::Match(Expression *expr, vector<Expression *> &bindings) {
	if (type && !type->Match(expr->return_type)) {
		return false;
	}
	if (expr_type && !expr_type->Match(expr->type)) {
		return false;
	}
	if (expr_class != ExpressionClass::INVALID && expr_class != expr->GetExpressionClass()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool ExpressionEqualityMatcher::Match(Expression *expr, vector<Expression *> &bindings) {
	if (!Expression::Equals(expression, expr)) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool CaseExpressionMatcher::Match(Expression *expr_p, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	return true;
}

bool ComparisonExpressionMatcher::Match(Expression *expr_p, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto expr = (BoundComparisonExpression *)expr_p;
	vector<Expression *> expressions = {expr->left.get(), expr->right.get()};
	return SetMatcher::Match(matchers, expressions, bindings, policy);
}

bool CastExpressionMatcher::Match(Expression *expr_p, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	if (!matcher) {
		return true;
	}
	auto expr = (BoundCastExpression *)expr_p;
	return matcher->Match(expr->child.get(), bindings);
}

bool InClauseExpressionMatcher::Match(Expression *expr_p, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto expr = (BoundOperatorExpression *)expr_p;
	if (expr->type != ExpressionType::COMPARE_IN || expr->type == ExpressionType::COMPARE_NOT_IN) {
		return false;
	}
	return SetMatcher::Match(matchers, expr->children, bindings, policy);
}

bool ConjunctionExpressionMatcher::Match(Expression *expr_p, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto expr = (BoundConjunctionExpression *)expr_p;
	if (!SetMatcher::Match(matchers, expr->children, bindings, policy)) {
		return false;
	}
	return true;
}

bool FunctionExpressionMatcher::Match(Expression *expr_p, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto expr = (BoundFunctionExpression *)expr_p;
	if (!FunctionMatcher::Match(function, expr->function.name)) {
		return false;
	}
	if (!SetMatcher::Match(matchers, expr->children, bindings, policy)) {
		return false;
	}
	return true;
}

bool FoldableConstantMatcher::Match(Expression *expr, vector<Expression *> &bindings) {
	// we match on ANY expression that is a scalar expression
	if (!expr->IsFoldable()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

} // namespace duckdb
