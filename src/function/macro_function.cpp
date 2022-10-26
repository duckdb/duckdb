
#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

// MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {}

MacroFunction::MacroFunction(MacroType type) : type(type) {
}

string MacroFunction::ValidateArguments(MacroFunction &macro_def, const string &name, FunctionExpression &function_expr,
                                        vector<unique_ptr<ParsedExpression>> &positionals,
                                        unordered_map<string, unique_ptr<ParsedExpression>> &defaults) {

	// separate positional and default arguments
	for (auto &arg : function_expr.children) {
		if ((arg->type == ExpressionType::VALUE_CONSTANT || arg->type == ExpressionType::VALUE_PARAMETER) &&
		    !arg->alias.empty()) {
			// default argument
			if (macro_def.default_parameters.find(arg->alias) == macro_def.default_parameters.end()) {
				return StringUtil::Format("Macro %s does not have default parameter %s!", name, arg->alias);
			} else if (defaults.find(arg->alias) != defaults.end()) {
				return StringUtil::Format("Duplicate default parameters %s!", arg->alias);
			}
			defaults[arg->alias] = move(arg);
		} else if (!defaults.empty()) {
			return "Positional parameters cannot come after parameters with a default value!";
		} else {
			// positional argument
			positionals.push_back(move(arg));
		}
	}

	// validate if the right number of arguments was supplied
	string error;
	auto &parameters = macro_def.parameters;
	if (parameters.size() != positionals.size()) {
		error = StringUtil::Format(
		    "Macro function '%s(%s)' requires ", name,
		    StringUtil::Join(parameters, parameters.size(), ", ", [](const unique_ptr<ParsedExpression> &p) {
			    return ((ColumnRefExpression &)*p).column_names[0];
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

void MacroFunction::CopyProperties(MacroFunction &other) {
	other.type = type;
	for (auto &param : parameters) {
		other.parameters.push_back(param->Copy());
	}
	for (auto &kv : default_parameters) {
		other.default_parameters[kv.first] = kv.second->Copy();
	}
}

string MacroFunction::ToSQL(const string &schema, const string &name) {
	vector<string> param_strings;
	for (auto &param : parameters) {
		param_strings.push_back(param->ToString());
	}
	for (auto &named_param : default_parameters) {
		param_strings.push_back(StringUtil::Format("%s := %s", named_param.first, named_param.second->ToString()));
	}

	return StringUtil::Format("CREATE MACRO %s.%s(%s) AS ", schema, name, StringUtil::Join(param_strings, ", "));
}

} // namespace duckdb
