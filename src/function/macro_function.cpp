#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

string MacroFunction::ValidateArguments(MacroCatalogEntry &macro_func, FunctionExpression &function_expr) {
	string error;
	auto &parameters = macro_func.function->parameters;
	auto &arguments = function_expr.children;
	if (parameters.size() != arguments.size()) {
		error = StringUtil::Format(
		    "Macro function '%s(%s)' requires ", macro_func.name,
		    StringUtil::Join(parameters, parameters.size(), ", ", [](const unique_ptr<ParsedExpression> &p) {
			    return ((ColumnRefExpression &)*p).column_name;
		    }));
		error += parameters.size() == 1 ? "a single argument" : StringUtil::Format("%i arguments", parameters.size());
		error += ", but ";
		error +=
		    arguments.size() == 1 ? "a single argument was" : StringUtil::Format("%i arguments were", arguments.size());
		error += " provided.";
	}
	return error;
}

} // namespace duckdb
