#include "duckdb/planner/expression_iterator.hpp"

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/planner/tableref/list.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

void ExpressionIterator::EnumerateChildren(const Expression &expr,
                                           const std::function<void(const Expression &child)> &callback) {
	EnumerateChildren((Expression &)expr, [&](unique_ptr<Expression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateChildren(Expression &expr, const std::function<void(Expression &child)> &callback) {
	EnumerateChildren(expr, [&](unique_ptr<Expression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateChildren(Expression &expr,
                                           const std::function<void(unique_ptr<Expression> &child)> &callback) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr_expr = expr.Cast<BoundAggregateExpression>();
		for (auto &child : aggr_expr.children) {
			callback(child);
		}
		if (aggr_expr.filter) {
			callback(aggr_expr.filter);
		}
		if (aggr_expr.order_bys) {
			for (auto &order : aggr_expr.order_bys->orders) {
				callback(order.expression);
			}
		}
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = expr.Cast<BoundBetweenExpression>();
		callback(between_expr.input);
		callback(between_expr.lower);
		callback(between_expr.upper);
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &case_check : case_expr.case_checks) {
			callback(case_check.when_expr);
			callback(case_check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		for (auto &child : func_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		for (auto &child : op_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &subquery_expr = expr.Cast<BoundSubqueryExpression>();
		for (auto &child : subquery_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = expr.Cast<BoundWindowExpression>();
		for (auto &partition : window_expr.partitions) {
			callback(partition);
		}
		for (auto &order : window_expr.orders) {
			callback(order.expression);
		}
		for (auto &child : window_expr.children) {
			callback(child);
		}
		if (window_expr.filter_expr) {
			callback(window_expr.filter_expr);
		}
		if (window_expr.start_expr) {
			callback(window_expr.start_expr);
		}
		if (window_expr.end_expr) {
			callback(window_expr.end_expr);
		}
		if (window_expr.offset_expr) {
			callback(window_expr.offset_expr);
		}
		if (window_expr.default_expr) {
			callback(window_expr.default_expr);
		}
		for (auto &order : window_expr.arg_orders) {
			callback(order.expression);
		}
		break;
	}
	case ExpressionClass::BOUND_UNNEST: {
		auto &unnest_expr = expr.Cast<BoundUnnestExpression>();
		callback(unnest_expr.child);
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		// these node types have no children
		break;
	default:
		throw InternalException("ExpressionIterator used on unbound expression");
	}
}

void ExpressionIterator::EnumerateExpression(unique_ptr<Expression> &expr,
                                             const std::function<void(Expression &child)> &callback) {
	if (!expr) {
		return;
	}
	callback(*expr);
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { EnumerateExpression(child, callback); });
}

void ExpressionIterator::EnumerateExpression(unique_ptr<Expression> &expr,
                                             const std::function<void(unique_ptr<Expression> &child)> &callback) {
	if (!expr) {
		return;
	}
	callback(expr);
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { EnumerateExpression(child, callback); });
}

void ExpressionIterator::VisitExpressionClass(const Expression &expr, ExpressionClass expr_class,
                                              const std::function<void(const Expression &child)> &callback) {
	if (expr.GetExpressionClass() == expr_class) {
		callback(expr);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { VisitExpressionClass(child, expr_class, callback); });
}

void ExpressionIterator::VisitExpressionClassMutable(
    unique_ptr<Expression> &expr, ExpressionClass expr_class,
    const std::function<void(unique_ptr<Expression> &child)> &callback) {
	if (expr->GetExpressionClass() == expr_class) {
		callback(expr);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { VisitExpressionClassMutable(child, expr_class, callback); });
}

} // namespace duckdb
