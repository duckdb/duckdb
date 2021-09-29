#include "duckdb/parser/parsed_expression_iterator.hpp"

#include "duckdb/parser/expression/list.hpp"

namespace duckdb {

void ParsedExpressionIterator::EnumerateChildren(const ParsedExpression &expression,
                                                 const std::function<void(const ParsedExpression &child)> &callback) {
	EnumerateChildren((ParsedExpression &)expression, [&](unique_ptr<ParsedExpression> &child) {
		D_ASSERT(child);
		callback(*child);
	});
}

void ParsedExpressionIterator::EnumerateChildren(ParsedExpression &expr,
                                                 const std::function<void(ParsedExpression &child)> &callback) {
	EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> &child) {
		D_ASSERT(child);
		callback(*child);
	});
}

void ParsedExpressionIterator::EnumerateChildren(
    ParsedExpression &expr, const std::function<void(unique_ptr<ParsedExpression> &child)> &callback) {
	switch (expr.expression_class) {
	case ExpressionClass::BETWEEN: {
		auto &cast_expr = (BetweenExpression &)expr;
		callback(cast_expr.input);
		callback(cast_expr.lower);
		callback(cast_expr.upper);
		break;
	}
	case ExpressionClass::CASE: {
		auto &case_expr = (CaseExpression &)expr;
		for (auto &check : case_expr.case_checks) {
			callback(check.when_expr);
			callback(check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast_expr = (CastExpression &)expr;
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &cast_expr = (CollateExpression &)expr;
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::COMPARISON: {
		auto &comp_expr = (ComparisonExpression &)expr;
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conj_expr = (ConjunctionExpression &)expr;
		for (auto &child : conj_expr.children) {
			callback(child);
		}
		break;
	}

	case ExpressionClass::FUNCTION: {
		auto &func_expr = (FunctionExpression &)expr;
		for (auto &child : func_expr.children) {
			callback(child);
		}
		if (func_expr.filter) {
			callback(func_expr.filter);
		}
		if (func_expr.order_bys) {
			for (auto &order : func_expr.order_bys->orders) {
				callback(order.expression);
			}
		}
		break;
	}
	case ExpressionClass::LAMBDA: {
		auto &lambda_expr = (LambdaExpression &)expr;
		callback(lambda_expr.expression);
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op_expr = (OperatorExpression &)expr;
		for (auto &child : op_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &subquery_expr = (SubqueryExpression &)expr;
		if (subquery_expr.child) {
			callback(subquery_expr.child);
		}
		break;
	}
	case ExpressionClass::WINDOW: {
		auto &window_expr = (WindowExpression &)expr;
		for (auto &partition : window_expr.partitions) {
			callback(partition);
		}
		for (auto &order : window_expr.orders) {
			callback(order.expression);
		}
		for (auto &child : window_expr.children) {
			callback(child);
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
		break;
	}
	case ExpressionClass::BOUND_EXPRESSION:
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::CONSTANT:
	case ExpressionClass::DEFAULT:
	case ExpressionClass::STAR:
	case ExpressionClass::PARAMETER:
	case ExpressionClass::POSITIONAL_REFERENCE:
		// these node types have no children
		break;
	default:
		// called on non ParsedExpression type!
		throw NotImplementedException("Unimplemented expression class");
	}
}

} // namespace duckdb
