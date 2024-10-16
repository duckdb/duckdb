
#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

MacroFunction::MacroFunction(MacroType type) : type(type) {
}

string FormatMacroFunction(MacroFunction &function, const string &name) {
	string result;
	result = name + "(";
	string parameters;
	for (auto &param : function.parameters) {
		if (!parameters.empty()) {
			parameters += ", ";
		}
		parameters += param->Cast<ColumnRefExpression>().GetColumnName();
	}
	for (auto &named_param : function.default_parameters) {
		if (!parameters.empty()) {
			parameters += ", ";
		}
		parameters += named_param.first;
		parameters += " := ";
		parameters += named_param.second->ToString();
	}
	result += parameters + ")";
	return result;
}

MacroBindResult MacroFunction::BindMacroFunction(const vector<unique_ptr<MacroFunction>> &functions, const string &name,
                                                 FunctionExpression &function_expr,
                                                 vector<unique_ptr<ParsedExpression>> &positionals,
                                                 unordered_map<string, unique_ptr<ParsedExpression>> &defaults) {
	// separate positional and default arguments
	for (auto &arg : function_expr.children) {
		if (!arg->alias.empty()) {
			// default argument
			if (defaults.count(arg->alias)) {
				return MacroBindResult(StringUtil::Format("Duplicate default parameters %s!", arg->alias));
			}
			defaults[arg->alias] = std::move(arg);
		} else if (!defaults.empty()) {
			return MacroBindResult("Positional parameters cannot come after parameters with a default value!");
		} else {
			// positional argument
			positionals.push_back(std::move(arg));
		}
	}

	// check for each macro function if it matches the number of positional arguments
	optional_idx result_idx;
	for (idx_t function_idx = 0; function_idx < functions.size(); function_idx++) {
		if (functions[function_idx]->parameters.size() == positionals.size()) {
			// found a matching function
			result_idx = function_idx;
			break;
		}
	}
	if (!result_idx.IsValid()) {
		// no matching function found
		string error;
		if (functions.size() == 1) {
			// we only have one function - print the old more detailed error message
			auto &macro_def = *functions[0];
			auto &parameters = macro_def.parameters;
			error = StringUtil::Format("Macro function %s requires ", FormatMacroFunction(macro_def, name));
			error += parameters.size() == 1 ? "a single positional argument"
			                                : StringUtil::Format("%i positional arguments", parameters.size());
			error += ", but ";
			error += positionals.size() == 1 ? "a single positional argument was"
			                                 : StringUtil::Format("%i positional arguments were", positionals.size());
			error += " provided.";
		} else {
			// we have multiple functions - list all candidates
			error += StringUtil::Format("Macro \"%s\" does not support %llu parameters.\n", name, positionals.size());
			error += "Candidate macros:";
			for (auto &function : functions) {
				error += "\n\t" + FormatMacroFunction(*function, name);
			}
		}
		return MacroBindResult(error);
	}
	// found a matching index - check if the default values exist within the macro
	auto macro_idx = result_idx.GetIndex();
	auto &macro_def = *functions[macro_idx];
	for (auto &default_val : defaults) {
		auto entry = macro_def.default_parameters.find(default_val.first);
		if (entry == macro_def.default_parameters.end()) {
			string error =
			    StringUtil::Format("Macro \"%s\" does not have a named parameter \"%s\"\n", name, default_val.first);
			error += "\nMacro definition: " + FormatMacroFunction(macro_def, name);
			return MacroBindResult(error);
		}
	}
	// Add the default values for parameters that have defaults, that were not explicitly assigned to
	for (auto it = macro_def.default_parameters.begin(); it != macro_def.default_parameters.end(); it++) {
		auto &parameter_name = it->first;
		auto &parameter_default = it->second;
		if (!defaults.count(parameter_name)) {
			// This parameter was not set yet, set it with the default value
			defaults[parameter_name] = parameter_default->Copy();
		}
	}
	return MacroBindResult(macro_idx);
}

void MacroFunction::CopyProperties(MacroFunction &other) const {
	other.type = type;
	for (auto &param : parameters) {
		other.parameters.push_back(param->Copy());
	}
	for (auto &kv : default_parameters) {
		other.default_parameters[kv.first] = kv.second->Copy();
	}
}

string MacroFunction::ToSQL() const {
	vector<string> param_strings;
	for (auto &param : parameters) {
		param_strings.push_back(param->ToString());
	}
	for (auto &named_param : default_parameters) {
		param_strings.push_back(StringUtil::Format("%s := %s", named_param.first, named_param.second->ToString()));
	}
	return StringUtil::Format("(%s) AS ", StringUtil::Join(param_strings, ", "));
}

} // namespace duckdb
