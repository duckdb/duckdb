#include "duckdb/parser/parsed_expression_iterator.hpp"

#include "duckdb/parser/expression/list.hpp"

namespace duckdb {
using namespace std;

void ParsedExpressionIterator::EnumerateChildren(const ParsedExpression &expression,
                                                 function<void(const ParsedExpression &child)> callback) {
	EnumerateChildren((ParsedExpression &)expression,
	                  [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
		                  callback(*child);
		                  return move(child);
	                  });
}

void ParsedExpressionIterator::EnumerateChildren(ParsedExpression &expr,
                                                 function<void(ParsedExpression &child)> callback) {
	EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
		callback(*child);
		return child;
	});
}

void ParsedExpressionIterator::EnumerateChildren(
    ParsedExpression &expr, function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> child)> callback) {
	switch (expr.expression_class) {
	case ExpressionClass::CASE: {
		auto &case_expr = (CaseExpression &)expr;
		case_expr.check = callback(move(case_expr.check));
		case_expr.result_if_true = callback(move(case_expr.result_if_true));
		case_expr.result_if_false = callback(move(case_expr.result_if_false));
		break;
	}
	case ExpressionClass::CAST: {
		auto &cast_expr = (CastExpression &)expr;
		cast_expr.child = callback(move(cast_expr.child));
		break;
	}
	case ExpressionClass::COLLATE: {
		auto &cast_expr = (CollateExpression &)expr;
		cast_expr.child = callback(move(cast_expr.child));
		break;
	}
	case ExpressionClass::COMPARISON: {
		auto &comp_expr = (ComparisonExpression &)expr;
		comp_expr.left = callback(move(comp_expr.left));
		comp_expr.right = callback(move(comp_expr.right));
		break;
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conj_expr = (ConjunctionExpression &)expr;
		for (auto &child : conj_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::FUNCTION: {
		auto &func_expr = (FunctionExpression &)expr;
		for (auto &child : func_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::OPERATOR: {
		auto &op_expr = (OperatorExpression &)expr;
		for (auto &child : op_expr.children) {
			child = callback(move(child));
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &subquery_expr = (SubqueryExpression &)expr;
		if (subquery_expr.child) {
			subquery_expr.child = callback(move(subquery_expr.child));
		}
		break;
	}
	case ExpressionClass::WINDOW: {
		auto &window_expr = (WindowExpression &)expr;
		for (auto &partition : window_expr.partitions) {
			partition = callback(move(partition));
		}
		for (auto &order : window_expr.orders) {
			order.expression = callback(move(order.expression));
		}
		for (auto &child : window_expr.children) {
			child = callback(move(child));
		}
		if (window_expr.offset_expr) {
			window_expr.offset_expr = callback(move(window_expr.offset_expr));
		}
		if (window_expr.default_expr) {
			window_expr.default_expr = callback(move(window_expr.default_expr));
		}
		break;
	}
	case ExpressionClass::BOUND_EXPRESSION:
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::CONSTANT:
	case ExpressionClass::DEFAULT:
	case ExpressionClass::STAR:
	case ExpressionClass::TABLE_STAR:
	case ExpressionClass::PARAMETER:
		// these node types have no children
		break;
	default:
		// called on non ParsedExpression type!
		throw NotImplementedException("Unimplemented expression class");
	}
}

} // namespace duckdb
