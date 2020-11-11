#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

unique_ptr<Expression> MacroFunction::BindMacroFunction(Binder &binder, ExpressionBinder &expr_binder,
                                                        MacroFunctionCatalogEntry &function,
                                                        vector<unique_ptr<Expression>> arguments, string &error) {
	// verify correct number of arguments
	auto &macro_func = function.function;
	auto &parameters = macro_func->parameters;
	if (parameters.size() != arguments.size()) {
		error = StringUtil::Format(
		    "Macro function '%s(%s)' requires ", function.name,
		    StringUtil::Join(parameters, parameters.size(), ", ", [](const unique_ptr<ParsedExpression> &p) {
			    return ((ColumnRefExpression &)*p).column_name;
		    }));
		error += parameters.size() == 1 ? "a single argument" : StringUtil::Format("%i arguments", parameters.size());
		error += ", but ";
		error +=
		    arguments.size() == 1 ? "a single argument was" : StringUtil::Format("%i arguments were", arguments.size());
		error += " provided.";
		return nullptr;
	}

	// check for arguments with side-effects
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &bound_func = (BoundFunctionExpression &)*arguments[i];
			if (bound_func.function.has_side_effects) {
				error = StringUtil::Format("Arguments with side-effects are not supported (you supplied '%s'). As a "
				                           "workaround, you can create a CTE.",
				                           bound_func.ToString());
				return nullptr;
			}
		}
	}

	// create macro_binder in binder to bind parameters to arguments
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < parameters.size(); i++) {
		types.push_back(arguments[i]->return_type);
		auto &param = (ColumnRefExpression &)*parameters[i];
		names.push_back(param.column_name);
	}
	binder.macro_binding = make_shared<MacroBinding>(types, names);
	binder.macro_binding->arguments = move(arguments);

	// now we perform the binding
	auto parsed_expression = macro_func->expression->Copy();
	auto result = expr_binder.Bind(parsed_expression);

	// delete the macro binding so that it cannot effect bindings outside of the macro call
	binder.macro_binding.reset();

	return result;
}

} // namespace duckdb
