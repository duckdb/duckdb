#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

// for nested function expressions
static ParsedExpression &GetParsedExpressionRecursive(ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_EXPRESSION)
		return expr;
	auto &bound_expr = (BoundExpression &)expr;
	return GetParsedExpressionRecursive(*bound_expr.parsed_expr);
}

unique_ptr<Expression> MacroFunction::BindMacroFunction(ExpressionBinder &binder, MacroFunctionCatalogEntry &function,
                                                        vector<unique_ptr<ParsedExpression>> children) {
	// replace arguments with those that were supplied
	auto &macro_func = function.function;
	auto parsed_expression = macro_func->expression->Copy();
	for (idx_t i = 0; i < children.size(); i++) {
		auto &argument = GetParsedExpressionRecursive(*children[i]);
		ParsedExpressionIterator::EnumerateChildren(
		    *parsed_expression, [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
			    if (child->Equals(macro_func->arguments[i].get())) {
				    return argument.Copy();
			    }
			    return child;
		    });
	}

	// now we perform the binding
	return binder.Bind(parsed_expression);
}

} // namespace duckdb
