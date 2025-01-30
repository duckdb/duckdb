#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

using expression_list_t = vector<unique_ptr<ParsedExpression>>;

static void AddChild(unique_ptr<ParsedExpression> &child, expression_list_t &new_children,
                     expression_list_t &replacements) {
	if (!StarExpression::IsColumnsUnpacked(*child)) {
		// Just add the child directly
		new_children.push_back(std::move(child));
		return;
	}
	// Replace the child with the replacement expression(s)
	for (auto &replacement : replacements) {
		new_children.push_back(replacement->Copy());
	}
}

static void ReplaceInFunction(unique_ptr<ParsedExpression> &expr, expression_list_t &star_list) {
	auto &function_expr = expr->Cast<FunctionExpression>();

	// Replace children
	expression_list_t new_children;
	for (auto &child : function_expr.children) {
		AddChild(child, new_children, star_list);
	}
	function_expr.children = std::move(new_children);

	// Replace ORDER_BY
	if (function_expr.order_bys) {
		expression_list_t new_orders;
		for (auto &order : function_expr.order_bys->orders) {
			AddChild(order.expression, new_orders, star_list);
		}
		if (new_orders.size() != function_expr.order_bys->orders.size()) {
			throw NotImplementedException("*COLUMNS(...) is not supported in the order expression");
		}
		for (idx_t i = 0; i < new_orders.size(); i++) {
			auto &new_order = new_orders[i];
			function_expr.order_bys->orders[i].expression = std::move(new_order);
		}
	}
}

static void ReplaceInOperator(unique_ptr<ParsedExpression> &expr, expression_list_t &star_list) {
	auto &operator_expr = expr->Cast<OperatorExpression>();

	vector<ExpressionType> allowed_types({
	    ExpressionType::OPERATOR_COALESCE,
	    ExpressionType::COMPARE_IN,
	    ExpressionType::COMPARE_NOT_IN,
	});
	bool allowed = false;
	for (idx_t i = 0; i < allowed_types.size() && !allowed; i++) {
		auto &type = allowed_types[i];
		if (operator_expr.type == type) {
			allowed = true;
		}
	}
	if (!allowed) {
		throw BinderException("*COLUMNS() can not be used together with the '%s' operator",
		                      EnumUtil::ToString(operator_expr.type));
	}

	// Replace children
	expression_list_t new_children;
	for (auto &child : operator_expr.children) {
		AddChild(child, new_children, star_list);
	}
	operator_expr.children = std::move(new_children);
}

void Binder::ReplaceUnpackedStarExpression(unique_ptr<ParsedExpression> &expr, expression_list_t &star_list) {
	D_ASSERT(expr);
	auto expression_class = expr->GetExpressionClass();
	// Replace *COLUMNS(...) in the supported places
	switch (expression_class) {
	case ExpressionClass::STAR: {
		if (!StarExpression::IsColumnsUnpacked(*expr)) {
			break;
		}
		// Deal with any *COLUMNS that was not replaced
		throw BinderException("*COLUMNS() can not be used in this place");
	}
	case ExpressionClass::FUNCTION: {
		ReplaceInFunction(expr, star_list);
		break;
	}
	case ExpressionClass::OPERATOR: {
		ReplaceInOperator(expr, star_list);
		break;
	}
	default: {
		break;
	}
	}

	// Visit the children of this expression, collecting the unpacked expressions
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { ReplaceUnpackedStarExpression(child_expr, star_list); });
}

} // namespace duckdb
