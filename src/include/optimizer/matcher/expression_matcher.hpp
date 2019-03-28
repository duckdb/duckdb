//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/matcher/expression_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "optimizer/matcher/expression_type_matcher.hpp"
#include "optimizer/matcher/set_matcher.hpp"
#include "optimizer/matcher/type_matcher.hpp"
#include "planner/expression/list.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! The ExpressionMatcher class contains a set of matchers that can be used to pattern match Expressions
class ExpressionMatcher {
public:
	ExpressionMatcher(ExpressionClass type = ExpressionClass::INVALID) : expr_class(type) {
	}
	virtual ~ExpressionMatcher() {
	}

	//! Checks if the given expression matches this ExpressionMatcher. If it does, the expression is appended to the
	//! bindings list and true is returned. Otherwise, false is returned.
	virtual bool Match(Expression *expr, vector<Expression *> &bindings) {
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

	//! The ExpressionClass of the to-be-matched expression. ExpressionClass::INVALID for ANY.
	ExpressionClass expr_class;
	//! Matcher for the ExpressionType of the operator (nullptr for ANY)
	unique_ptr<ExpressionTypeMatcher> expr_type;
	//! Matcher for the return_type of the expression (nullptr for ANY)
	unique_ptr<TypeMatcher> type;
};

//! The ExpressionEqualityMatcher matches on equality with another (given) expression
class ExpressionEqualityMatcher : public ExpressionMatcher {
public:
	ExpressionEqualityMatcher(Expression *expr) : ExpressionMatcher(ExpressionClass::INVALID), expression(expr) {
	}

	bool Match(Expression *expr, vector<Expression *> &bindings) override {
		if (!Expression::Equals(expression, expr)) {
			return false;
		}
		bindings.push_back(expr);
		return true;
	}

private:
	Expression *expression;
};

class ConstantExpressionMatcher : public ExpressionMatcher {
public:
	ConstantExpressionMatcher() : ExpressionMatcher(ExpressionClass::CONSTANT) {
	}
};

class CaseExpressionMatcher : public ExpressionMatcher {
public:
	CaseExpressionMatcher() : ExpressionMatcher(ExpressionClass::CASE) {
	}
	//! The check expression to match (if any)
	unique_ptr<ExpressionMatcher> check;
	//! The result_if_true expression to match (if any)
	unique_ptr<ExpressionMatcher> result_if_true;
	//! The result_if_false expression to match (if any)
	unique_ptr<ExpressionMatcher> result_if_false;

	bool Match(Expression *expr_, vector<Expression *> &bindings) override {
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
};

class CastExpressionMatcher : public ExpressionMatcher {
public:
	CastExpressionMatcher() : ExpressionMatcher(ExpressionClass::CAST) {
	}
	//! The child expression to match (if any)
	unique_ptr<ExpressionMatcher> child;

	bool Match(Expression *expr_, vector<Expression *> &bindings) override {
		if (!ExpressionMatcher::Match(expr_, bindings)) {
			return false;
		}
		auto expr = (BoundCastExpression *)expr_;
		if (child && !child->Match(expr->child.get(), bindings)) {
			return false;
		}
		return true;
	}
};

class ComparisonExpressionMatcher : public ExpressionMatcher {
public:
	ComparisonExpressionMatcher() : ExpressionMatcher(ExpressionClass::COMPARISON) {
	}
	//! The matchers for the child expressions
	vector<unique_ptr<ExpressionMatcher>> matchers;
	//! The set matcher matching policy to use
	SetMatcher::Policy policy;

	bool Match(Expression *expr_, vector<Expression *> &bindings) override {
		if (!ExpressionMatcher::Match(expr_, bindings)) {
			return false;
		}
		auto expr = (BoundComparisonExpression *)expr_;
		vector<Expression *> expressions = {expr->left.get(), expr->right.get()};
		return SetMatcher::Match(matchers, expressions, bindings, policy);
	}
};

class ConjunctionExpressionMatcher : public ExpressionMatcher {
public:
	ConjunctionExpressionMatcher() : ExpressionMatcher(ExpressionClass::CONJUNCTION) {
	}
	//! The matchers for the child expressions
	vector<unique_ptr<ExpressionMatcher>> matchers;
	//! The set matcher matching policy to use
	SetMatcher::Policy policy;

	bool Match(Expression *expr_, vector<Expression *> &bindings) override {
		if (!ExpressionMatcher::Match(expr_, bindings)) {
			return false;
		}
		auto expr = (BoundConjunctionExpression *)expr_;
		vector<Expression *> expressions = {expr->left.get(), expr->right.get()};
		return SetMatcher::Match(matchers, expressions, bindings, policy);
	}
};

class OperatorExpressionMatcher : public ExpressionMatcher {
public:
	OperatorExpressionMatcher() : ExpressionMatcher(ExpressionClass::OPERATOR) {
	}
	//! The matchers for the child expressions
	vector<unique_ptr<ExpressionMatcher>> matchers;
	//! The set matcher matching policy to use
	SetMatcher::Policy policy;

	bool Match(Expression *expr_, vector<Expression *> &bindings) override {
		if (!ExpressionMatcher::Match(expr_, bindings)) {
			return false;
		}
		auto expr = (BoundOperatorExpression *)expr_;
		return SetMatcher::Match(matchers, expr->children, bindings, policy);
	}
};

//! The FoldableConstant matcher matches any expression that is foldable into a constant by the ExpressionExecutor (i.e.
//! scalar but not aggregate/window/parameter)
class FoldableConstantMatcher : public ExpressionMatcher {
public:
	FoldableConstantMatcher() : ExpressionMatcher(ExpressionClass::INVALID) {
	}

	bool Match(Expression *expr, vector<Expression *> &bindings) override {
		// we match on ANY expression that is a scalar expression
		if (!expr->IsScalar()) {
			return false;
		}
		// ...except if it is an Aggregate or Window function
		if (expr->IsAggregate() || expr->IsWindow()) {
			return false;
		}
		// also, if an expression contains a parameter for a prepared statement anywhere, we do not match
		if (expr->HasParameter()) {
			return false;
		}
		bindings.push_back(expr);
		return true;
	}
};

} // namespace duckdb
