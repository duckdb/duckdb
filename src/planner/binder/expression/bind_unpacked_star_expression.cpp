#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

using expression_list_t = vector<unique_ptr<ParsedExpression>>;

static void AddChild(unique_ptr<ParsedExpression> &child, expression_list_t &new_children,
                     expression_list_t &replacements, StarExpression &star, optional_ptr<duckdb_re2::RE2> regex) {
	if (!StarExpression::IsColumnsUnpacked(*child)) {
		// Just add the child directly
		new_children.push_back(std::move(child));
		return;
	}
	auto &unpack = child->Cast<OperatorExpression>();
	D_ASSERT(unpack.GetExpressionType() == ExpressionType::OPERATOR_UNPACK);
	D_ASSERT(unpack.GetChildren().size() == 1);
	auto &unpack_child = unpack.GetChildrenMutable()[0];

	// Replace the child with the replacement expression(s)
	for (auto &replacement : replacements) {
		auto new_expr = unpack_child->Copy();
		Binder::ReplaceStarExpression(new_expr, replacement);
		if (StarExpression::IsColumns(star)) {
			auto expr = Binder::GetResolvedColumnExpression(*replacement);
			if (expr) {
				auto &colref = expr->Cast<ColumnRefExpression>();
				if (new_expr->GetAlias().empty()) {
					new_expr->SetAlias(colref.GetColumnName());
				} else {
					new_expr->SetAlias(Identifier(Binder::ReplaceColumnsAlias(
					    new_expr->GetAlias().GetIdentifierName(), colref.GetColumnName().GetIdentifierName(), regex)));
				}
			}
		}
		new_children.push_back(std::move(new_expr));
	}
}

static void ReplaceInFunction(unique_ptr<ParsedExpression> &expr, expression_list_t &star_list, StarExpression &star,
                              optional_ptr<duckdb_re2::RE2> regex) {
	auto &function_expr = expr->Cast<FunctionExpression>();

	// Replace children, keeping the name of any argument that expanded into exactly one expression
	vector<FunctionArgument> new_arguments;
	for (auto &child : function_expr.GetArgumentsMutable()) {
		expression_list_t expanded;
		AddChild(child.GetExpressionMutable(), expanded, star_list, star, regex);
		if (expanded.size() == 1) {
			new_arguments.emplace_back(child.GetName(), std::move(expanded[0]));
			continue;
		}
		for (auto &new_child : expanded) {
			new_arguments.emplace_back(std::move(new_child));
		}
	}
	function_expr.GetArgumentsMutable() = std::move(new_arguments);
	// the expansion may have produced column references struct_pack can name itself
	function_expr.ApplyImplicitFieldNames();

	// Replace ORDER_BY
	if (function_expr.OrderBy()) {
		expression_list_t new_orders;
		for (auto &order : function_expr.OrderByMutable()->orders) {
			AddChild(order.expression, new_orders, star_list, star, regex);
		}
		if (new_orders.size() != function_expr.OrderBy()->orders.size()) {
			throw NotImplementedException("*COLUMNS(...) is not supported in the order expression");
		}
		for (idx_t i = 0; i < new_orders.size(); i++) {
			auto &new_order = new_orders[i];
			function_expr.OrderByMutable()->orders[i].expression = std::move(new_order);
		}
	}
}

static void ReplaceInOperator(unique_ptr<ParsedExpression> &expr, expression_list_t &star_list, StarExpression &star,
                              optional_ptr<duckdb_re2::RE2> regex) {
	auto &operator_expr = expr->Cast<OperatorExpression>();

	vector<ExpressionType> allowed_types({
	    ExpressionType::OPERATOR_COALESCE,
	    ExpressionType::COMPARE_IN,
	    ExpressionType::COMPARE_NOT_IN,
	});
	bool allowed = false;
	for (idx_t i = 0; i < allowed_types.size() && !allowed; i++) {
		auto &type = allowed_types[i];
		if (operator_expr.GetExpressionType() == type) {
			allowed = true;
		}
	}
	if (!allowed) {
		throw BinderException("*COLUMNS() can not be used together with the '%s' operator",
		                      EnumUtil::ToString(operator_expr.GetExpressionType()));
	}

	// Replace children
	expression_list_t new_children;
	for (auto &child : operator_expr.GetChildrenMutable()) {
		AddChild(child, new_children, star_list, star, regex);
	}
	operator_expr.GetChildrenMutable() = std::move(new_children);
}

void Binder::ReplaceUnpackedStarExpression(unique_ptr<ParsedExpression> &expr, expression_list_t &star_list,
                                           StarExpression &star, optional_ptr<duckdb_re2::RE2> regex) {
	D_ASSERT(expr);
	auto expression_class = expr->GetExpressionClass();
	// Replace *COLUMNS(...) in the supported places
	switch (expression_class) {
	case ExpressionClass::FUNCTION: {
		ReplaceInFunction(expr, star_list, star, regex);
		break;
	}
	case ExpressionClass::OPERATOR: {
		if (StarExpression::IsColumnsUnpacked(*expr)) {
			throw BinderException("*COLUMNS() can not be used in this place");
		}
		ReplaceInOperator(expr, star_list, star, regex);
		break;
	}
	default: {
		break;
	}
	}

	// Visit the children of this expression, collecting the unpacked expressions
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child_expr) {
		ReplaceUnpackedStarExpression(child_expr, star_list, star, regex);
	});
}

} // namespace duckdb
