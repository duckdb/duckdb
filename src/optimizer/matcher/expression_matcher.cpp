#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/planner/expression/list.hpp"

using namespace duckdb;
using namespace std;

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

bool CaseExpressionMatcher::Match(Expression *expr_, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_, bindings)) {
		return false;
	}
	auto expr = (BoundCaseExpression *)expr_;
	if (check && !check->Match(expr->check.get(), bindings)) {
		return false;
	}
	if (result_if_true && !result_if_true->Match(expr->result_if_true.get(), bindings)) {
		return false;
	}
	if (result_if_false && !result_if_false->Match(expr->result_if_false.get(), bindings)) {
		return false;
	}
	return true;
}

bool CastExpressionMatcher::Match(Expression *expr_, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_, bindings)) {
		return false;
	}
	auto expr = (BoundCastExpression *)expr_;
	if (child && !child->Match(expr->child.get(), bindings)) {
		return false;
	}
	return true;
}

bool ComparisonExpressionMatcher::Match(Expression *expr_, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_, bindings)) {
		return false;
	}
	auto expr = (BoundComparisonExpression *)expr_;
	vector<Expression *> expressions = {expr->left.get(), expr->right.get()};
	return SetMatcher::Match(matchers, expressions, bindings, policy);
}

bool ConjunctionExpressionMatcher::Match(Expression *expr_, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_, bindings)) {
		return false;
	}
	auto expr = (BoundConjunctionExpression *)expr_;
	if (!SetMatcher::Match(matchers, expr->children, bindings, policy)) {
		return false;
	}
	return true;
}

bool OperatorExpressionMatcher::Match(Expression *expr_, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_, bindings)) {
		return false;
	}
	auto expr = (BoundOperatorExpression *)expr_;
	return SetMatcher::Match(matchers, expr->children, bindings, policy);
}

bool FunctionExpressionMatcher::Match(Expression *expr_, vector<Expression *> &bindings) {
	if (!ExpressionMatcher::Match(expr_, bindings)) {
		return false;
	}
	auto expr = (BoundFunctionExpression *)expr_;
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
