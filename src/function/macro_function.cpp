#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

MacroFunction::MacroFunction(MacroType type) : type(type) {
}

string FormatMacroFunction(const MacroFunction &function, const string &name) {
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

MacroBindResult
MacroFunction::BindMacroFunction(const vector<unique_ptr<MacroFunction>> &functions, const string &name,
                                 FunctionExpression &function_expr,
                                 vector<unique_ptr<ParsedExpression>> &positional_arguments,
                                 case_insensitive_map_t<unique_ptr<ParsedExpression>> &named_arguments) {
	// separate positional and default arguments
	for (auto &arg : function_expr.children) {
		if (!arg->GetAlias().empty()) {
			// default argument
			if (named_arguments.find(arg->GetAlias()) != named_arguments.end()) {
				return MacroBindResult(
				    StringUtil::Format("Macro %s() has named argument repeated '%s'", name, arg->GetAlias()));
			}
			named_arguments[arg->GetAlias()] = std::move(arg);
		} else if (!named_arguments.empty()) {
			return MacroBindResult(
			    StringUtil::Format("Macro %s() has positional argument following named argument", name));
		} else {
			// positional argument
			positional_arguments.push_back(std::move(arg));
		}
	}

	// check for each macro function if it matches the number of positional arguments
	optional_idx result_idx;
	for (idx_t function_idx = 0; function_idx < functions.size(); function_idx++) {
		auto &function = functions[function_idx];
		if (positional_arguments.size() > function->parameters.size()) {
			continue; // Can't be a match, and avoids out-of-bound below
		}

		// Skip over positionals (needs loop once we implement typed macro overloads)
		idx_t param_idx = positional_arguments.size();

		// Match named and fill in defaults
		for (; param_idx < function->parameters.size(); param_idx++) {
			const auto &param_name = function->parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
			if (named_arguments.find(param_name) != named_arguments.end()) {
				continue; // The user has supplied a named argument for this parameter
			}
			if (function->default_parameters.find(param_name) != function->default_parameters.end()) {
				continue; // This parameter has a default argument
			}
			// The parameter has no argument!
			break;
		}

		if (param_idx == function->parameters.size()) {
			// Found a matching function
			result_idx = function_idx;
			break;
		}
	}

	if (!result_idx.IsValid()) {
		// No matching function found
		string error = StringUtil::Format("Macro %s() does not support the supplied arguments.\n", name);
		error += "Candidate macros:";
		for (auto &function : functions) {
			error += "\n\t" + FormatMacroFunction(*function, name);
		}
		return MacroBindResult(error);
	}

	// Found a matching function - check if all named arguments have a matching parameter
	const auto macro_idx = result_idx.GetIndex();
	const auto &macro_def = *functions[macro_idx];
	for (auto &named_argument : named_arguments) {
		bool found = false;
		for (const auto &parameter : macro_def.parameters) {
			if (StringUtil::CIEquals(named_argument.first, parameter->Cast<ColumnRefExpression>().GetColumnName())) {
				found = true;
				break;
			}
		}
		if (!found) {
			string error =
			    StringUtil::Format("Macro %s() got an unexpected named argument '%s'\n", name, named_argument.first);
			error += "\nMacro definition: " + FormatMacroFunction(macro_def, name);
			return MacroBindResult(error);
		}
	}

	// Add the default values for parameters that have defaults, that were not explicitly assigned to
	for (auto it = macro_def.default_parameters.begin(); it != macro_def.default_parameters.end(); it++) {
		auto &parameter_name = it->first;
		auto &parameter_default = it->second;
		if (!named_arguments.count(parameter_name)) {
			// This parameter was not set yet, set it with the default value
			named_arguments[parameter_name] = parameter_default->Copy();
		}
	}

	return MacroBindResult(macro_idx);
}

unique_ptr<DummyBinding>
MacroFunction::CreateDummyBinding(const MacroFunction &macro_def, const string &name,
                                  vector<unique_ptr<ParsedExpression>> &positional_arguments,
                                  case_insensitive_map_t<unique_ptr<ParsedExpression>> &named_arguments) {
	// create a MacroBinding to bind this macro's parameters to its arguments
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < positional_arguments.size(); i++) {
		types.emplace_back(LogicalTypeId::UNKNOWN);
		names.push_back(macro_def.parameters[i]->Cast<ColumnRefExpression>().GetColumnName());
	}
	for (auto &named_argument : named_arguments) {
		types.emplace_back(LogicalTypeId::UNKNOWN);
		names.push_back(named_argument.first);
		positional_arguments.push_back(std::move(named_argument.second)); // push defaults into positionals
	}

	auto res = make_uniq<DummyBinding>(types, names, name);
	res->arguments = &positional_arguments;
	return res;
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

vector<unique_ptr<ParsedExpression>>
MacroFunction::GetPositionalParametersForSerialization(Serializer &serializer) const {
	vector<unique_ptr<ParsedExpression>> result;
	if (serializer.ShouldSerialize(6)) {
		// We serialize all positional parameters as-is
		for (auto &param : parameters) {
			result.push_back(param->Copy());
		}
		return result;
	}
	// Serializing targeting an older version - delete all named parameters from the list of positional parmaeters
	for (auto &param : parameters) {
		auto &colref = param->Cast<ColumnRefExpression>();
		if (default_parameters.find(colref.GetName()) != default_parameters.end()) {
			// This is a default parameter - do not serialize
			continue;
		}
		result.push_back(param->Copy());
	}
	return result;
}

void MacroFunction::FinalizeDeserialization() {
	// TODO
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
