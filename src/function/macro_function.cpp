#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

string MacroFunction::ValidateArguments(MacroCatalogEntry &macro_func, FunctionExpression &function_expr,
                                        vector<unique_ptr<ParsedExpression>> &positionals,
                                        unordered_map<string, unique_ptr<ParsedExpression>> &defaults) {
	// separate positional and default arguments
	auto &macro_def = *macro_func.function;
	for (auto &arg : function_expr.children) {
		if (arg->type == ExpressionType::COMPARE_EQUAL) {
			// possibly default argument
			auto &comp_expr = (ComparisonExpression &)*arg;
			if (macro_def.default_parameters.find(comp_expr.left->ToString()) != macro_def.default_parameters.end()) {
				// it's a default arg!
				defaults[comp_expr.left->ToString()] = move(comp_expr.right);
				continue;
			}
		} else if (!defaults.empty()) {
			return "Positional parameters cannot come after parameters with a default value!";
		}
		// positional argument
		positionals.push_back(move(arg));
	}

	// validate if the right number of arguments was supplied
	string error;
	auto &parameters = macro_func.function->parameters;
	if (parameters.size() != positionals.size()) {
		error = StringUtil::Format(
		    "Macro function '%s(%s)' requires ", macro_func.name,
		    StringUtil::Join(parameters, parameters.size(), ", ", [](const unique_ptr<ParsedExpression> &p) {
			    return ((ColumnRefExpression &)*p).column_name;
		    }));
		error += parameters.size() == 1 ? "a single positional argument"
		                                : StringUtil::Format("%i positional arguments", parameters.size());
		error += ", but ";
		error += positionals.size() == 1 ? "a single positional argument was"
		                                 : StringUtil::Format("%i positional arguments were", positionals.size());
		error += " provided.";
		return error;
	}

	// fill in default value where this was not supplied
	for (auto it = macro_def.default_parameters.begin(); it != macro_def.default_parameters.end(); it++) {
		if (defaults.find(it->first) == defaults.end()) {
			defaults[it->first] = it->second->Copy();
		}
	}

	return error;
}

unique_ptr<MacroFunction> MacroFunction::Copy() {
	auto result = make_unique<MacroFunction>(expression->Copy());
	for (auto &param : parameters) {
		result->parameters.push_back(param->Copy());
	}
	for (auto &kv : default_parameters) {
		result->default_parameters[kv.first] = kv.second->Copy();
	}
	return result;
}

} // namespace duckdb
