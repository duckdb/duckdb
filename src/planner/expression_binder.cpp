#include "planner/expression_binder.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "planner/binder.hpp"
#include "planner/expression/bound_cast_expression.hpp"
#include "planner/expression/bound_expression.hpp"
#include "planner/expression/bound_subquery_expression.hpp"

using namespace duckdb;
using namespace std;

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder)
    : binder(binder), context(context), stored_binder(nullptr) {
	if (replace_binder) {
		stored_binder = binder.GetActiveBinder();
		binder.SetActiveBinder(this);
	} else {
		binder.PushExpressionBinder(this);
	}
}

ExpressionBinder::~ExpressionBinder() {
	if (binder.HasActiveBinder()) {
		if (stored_binder) {
			binder.SetActiveBinder(stored_binder);
		} else {
			binder.PopExpressionBinder();
		}
	}
}

unique_ptr<Expression> ExpressionBinder::GetExpression(ParsedExpression &expr) {
	assert(expr.GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION);
	assert(((BoundExpression &)expr).expr);
	return move(((BoundExpression &)expr).expr);
}

string ExpressionBinder::Bind(unique_ptr<ParsedExpression> *expr, uint32_t depth, bool root_expression) {
	// bind the node, but only if it has not been bound yet
	auto &expression = **expr;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
		// already bound, don't bind it again
		return string();
	}
	// bind the expression
	BindResult result = BindExpression(**expr, depth, root_expression);
	if (result.HasError()) {
		return result.error;
	} else {
		// successfully bound: replace the node with a BoundExpression
		*expr = make_unique<BoundExpression>(move(result.expression));
		return string();
	}
}

BindResult ExpressionBinder::BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression) {
	switch (expr.GetExpressionClass()) {
	// case ExpressionClass::AGGREGATE:
	// 	return BindExpression((AggregateExpression&) expr, depth);
	case ExpressionClass::CASE:
		return BindExpression((CaseExpression &)expr, depth);
	case ExpressionClass::CAST:
		return BindExpression((CastExpression &)expr, depth);
	case ExpressionClass::COLUMN_REF:
		return BindExpression((ColumnRefExpression &)expr, depth);
	case ExpressionClass::COMPARISON:
		return BindExpression((ComparisonExpression &)expr, depth);
	case ExpressionClass::CONJUNCTION:
		return BindExpression((ConjunctionExpression &)expr, depth);
	case ExpressionClass::CONSTANT:
		return BindExpression((ConstantExpression &)expr, depth);
	// case ExpressionClass::DEFAULT:
	// 	return BindExpression((DefaultExpression&) expr, depth);
	case ExpressionClass::FUNCTION:
		return BindExpression((FunctionExpression &)expr, depth);
	case ExpressionClass::OPERATOR:
		return BindExpression((OperatorExpression &)expr, depth);
	case ExpressionClass::SUBQUERY:
		return BindExpression((SubqueryExpression &)expr, depth);
	// case ExpressionClass::WINDOW:
	// 	return BindExpression((WindowExpression&) expr, depth);
	default:
		assert(expr.GetExpressionClass() == ExpressionClass::PARAMETER);
		return BindExpression((ParameterExpression &)expr, depth);
	}
}

unique_ptr<Expression> ExpressionBinder::BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr) {
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto &active_binders = binder.GetActiveBinders();
	// make a copy of the set of binders, so we can restore it later
	auto binders = active_binders;
	active_binders.pop_back();
	size_t depth = 1;
	unique_ptr<Expression> result;
	while (active_binders.size() > 0) {
		auto &next_binder = active_binders.back();
		auto bind_result = next_binder->BindExpression(*expr, depth);
		if (!bind_result.HasError()) {
			result = move(bind_result.expression);
			break;
		}
		depth++;
		active_binders.pop_back();
	}
	active_binders = binders;
	return result;
}

unique_ptr<Expression> ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr) {
	// bind the main expression
	auto result = BindExpression(*expr, 0, true);
	if (!result.HasError()) {
		return move(result.expression);
	}
	// failed to bind: try to bind correlated columns in the expression (if any)
	auto subquery_bind = BindCorrelatedColumns(expr);
	if (!subquery_bind) {
		throw BinderException(result.error);
	}
	return subquery_bind;
}

unique_ptr<Expression> ExpressionBinder::AddCastToType(unique_ptr<Expression> expr, SQLType target_type) {
	assert(expr);
	if (expr->GetExpressionClass() == ExpressionClass::PARAMETER || expr->sql_type != target_type) {
		return make_unique<BoundCastExpression>(GetInternalType(target_type), target_type, move(expr));
	}
	return expr;
}
