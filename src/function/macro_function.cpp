#include "duckdb/function/macro_function.hpp"

#include "duckdb/planner/expression/bound_macro_expression.hpp"
#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

unique_ptr<BoundMacroExpression> MacroFunction::BindMacroFunction(ClientContext &context,
                                                                  MacroFunctionCatalogEntry &function,
                                                                  vector<unique_ptr<Expression>> children,
                                                                  string &error) {
	auto &macro_func = function.function;

	auto bound_expression = macro_func->expression->Copy();
	ParsedExpressionIterator::EnumerateChildren(*bound_expression, [&](const ParsedExpression &child) {
		for (idx_t i = 0; i < macro_func->arguments.size(); i++) {
			auto &arg = macro_func->arguments[i];
			if (child.Equals(arg.get())) {
				child =
			}
		}
	});

	auto result = make_unique<BoundMacroExpression>(LogicalType::SQLNULL, function.name, nullptr, move(children));

	return result;
}

} // namespace duckdb
