#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

string MacroFunction::CheckArguments(ClientContext &context, QueryErrorContext &error_context,
                                     MacroFunctionCatalogEntry &macro_func, FunctionExpression &function_expr) {
	string error;
	auto &catalog = Catalog::GetCatalog(context);
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
		return error;
	}

	// check for arguments with side-effects TODO: to support this, a projection must be pushed
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i]->GetExpressionClass() == ExpressionClass::FUNCTION) {
			auto func_arg = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function_expr.schema,
			                                 function_expr.function_name, false, error_context);
			if (func_arg->type == CatalogType::SCALAR_FUNCTION_ENTRY) {
				auto &scalar_func_arg = (ScalarFunctionCatalogEntry &)*func_arg;
				for (auto sfa : scalar_func_arg.functions) {
					if (sfa.has_side_effects) {
						error = StringUtil::Format(
						    "Arguments with side-effects are not supported ('%s' was supplied). As a "
						    "workaround, try creating a CTE that evaluates the argument with side-effects.",
						    sfa.name);
						return error;
					}
				}
			}
		}
	}
	return error;
}

} // namespace duckdb
