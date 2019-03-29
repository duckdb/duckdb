#include "planner/expression_iterator.hpp"

#include "planner/expression/list.hpp"

using namespace duckdb;
using namespace std;

void ExpressionIterator::EnumerateChildren(const Expression &expr, function<void(const Expression &child)> callback) {
	EnumerateChildren((Expression &)expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		callback(*child);
		return child;
	});
}

void ExpressionIterator::EnumerateChildren(Expression &expr, std::function<void(Expression &child)> callback) {
	EnumerateChildren(expr, [&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		callback(*child);
		return child;
	});
}

void ExpressionIterator::EnumerateChildren(Expression &expr,
                                           function<unique_ptr<Expression>(unique_ptr<Expression> child)> callback) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr_expr = (BoundAggregateExpression &)expr;
		if (aggr_expr.child) {
			aggr_expr.child = callback(move(aggr_expr.child));
		}
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = (BoundCaseExpression &)expr;
		case_expr.check = callback(move(case_expr.check));
		case_expr.result_if_true = callback(move(case_expr.result_if_true));
		case_expr.result_if_false = callback(move(case_expr.result_if_false));
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = (BoundCastExpression &)expr;
		cast_expr.child = callback(move(cast_expr.child));
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = (BoundComparisonExpression &)expr;
		comp_expr.left = callback(move(comp_expr.left));
		comp_expr.right = callback(move(comp_expr.right));
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = (BoundConjunctionExpression &)expr;
		conj_expr.left = callback(move(conj_expr.left));
		conj_expr.right = callback(move(conj_expr.right));
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = (BoundFunctionExpression &)expr;
		for (auto &child : func_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = (BoundOperatorExpression &)expr;
		for (auto &child : op_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &subquery_expr = (BoundSubqueryExpression &)expr;
		if (subquery_expr.child) {
			subquery_expr.child = callback(move(subquery_expr.child));
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = (BoundWindowExpression &)expr;
		for (auto &partition : window_expr.partitions) {
			partition = callback(move(partition));
		}
		for (auto &order : window_expr.orders) {
			order.expression = callback(move(order.expression));
		}
		if (window_expr.child) {
			window_expr.child = callback(move(window_expr.child));
		}
		if (window_expr.offset_expr) {
			window_expr.offset_expr = callback(move(window_expr.offset_expr));
		}
		if (window_expr.default_expr) {
			window_expr.default_expr = callback(move(window_expr.default_expr));
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		// these node types have no children
		break;
	default:
		// called on non BoundExpression type!
		assert(0);
		break;
	}
}
