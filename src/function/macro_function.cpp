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
	auto result = name + "(";
	;
	string parameters;
	for (auto &param : function.parameters) {
		if (!parameters.empty()) {
			parameters += ", ";
		}
		const auto &param_name = param->Cast<ColumnRefExpression>().GetColumnName();
		parameters += param_name;

		auto it = function.default_parameters.find(param_name);
		if (it != function.default_parameters.end()) {
			parameters += " := ";
			parameters += it->second->ToString();
		}
	}
	result += parameters + ")";
	return result;
}

MacroBindResult
MacroFunction::BindMacroFunction(const vector<unique_ptr<MacroFunction>> &functions, const string &name,
                                 FunctionExpression &function_expr,
                                 vector<unique_ptr<ParsedExpression>> &positional_arguments,
                                 InsertionOrderPreservingMap<unique_ptr<ParsedExpression>> &named_arguments) {
	// Separate positional and default arguments
	for (auto &arg : function_expr.children) {
		if (!arg->GetAlias().empty()) {
			// Default argument
			if (named_arguments.find(arg->GetAlias()) != named_arguments.end()) {
				return MacroBindResult(
				    StringUtil::Format("Macro %s() has named argument repeated '%s'", name, arg->GetAlias()));
			}
			named_arguments[arg->GetAlias()] = std::move(arg);
		} else if (!named_arguments.empty()) {
			return MacroBindResult(
			    StringUtil::Format("Macro %s() has positional argument following named argument", name));
		} else {
			// Positional argument
			positional_arguments.push_back(std::move(arg));
		}
	}

	// Check for each macro function if it matches the number of positional arguments
	vector<idx_t> result_indices;
	for (idx_t function_idx = 0; function_idx < functions.size(); function_idx++) {
		auto &function = functions[function_idx];

		// Check if we can exclude the match based on argument count (also avoids out-of-bounds below)
		if (positional_arguments.size() > function->parameters.size() ||
		    positional_arguments.size() + named_arguments.size() > function->parameters.size()) {
			continue;
		}

		// Also check if we can exclude the match based on the supplied named arguments
		bool found_all_named_arguments = true;
		for (auto &kv : named_arguments) {
			bool found = false;
			for (const auto &parameter : function->parameters) {
				if (StringUtil::CIEquals(kv.first, parameter->Cast<ColumnRefExpression>().GetColumnName())) {
					found = true;
					break;
				}
			}
			if (!found) {
				found_all_named_arguments = false;
				break;
			}
		}
		if (!found_all_named_arguments) {
			continue; // One of the supplied named arguments is not present in the macro definition
		}

		// Loop through arguments, positionals first, then named
		idx_t param_idx = 0;

		// Figure out if any positional arguments are duplicated in named arguments
		bool duplicate = false;
		for (; param_idx < positional_arguments.size(); param_idx++) {
			const auto &param_name = function->parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
			if (named_arguments.find(param_name) != named_arguments.end()) {
				duplicate = true;
				break;
			}
		}
		if (duplicate) {
			continue;
		}

		// Match remaining arguments with named/defaults
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
			result_indices.push_back(function_idx);
		}
	}

	if (result_indices.size() != 1) {
		string error;
		if (result_indices.empty()) {
			// No matching function found
			error = StringUtil::Format("Macro %s() does not support the supplied arguments.\n", name);
		} else {
			// Multiple matching functions found
			error = StringUtil::Format("Macro %s() has multiple overloads that match the supplied arguments. ", name);
			error += "In order to select one, please supply all arguments by name.\n";
		}
		error += "Candidate macros:";
		for (auto &function : functions) {
			error += "\n\t" + FormatMacroFunction(*function, name);
		}
		return MacroBindResult(error);
	}

	const auto &macro_idx = result_indices[0];
	const auto &macro_def = *functions[macro_idx];

	// Skip over positionals (needs loop once we implement typed macro overloads)
	idx_t param_idx = positional_arguments.size();

	// Add the default values for parameters that have defaults, for which no argument was supplied
	for (; param_idx < macro_def.parameters.size(); param_idx++) {
		const auto &param_name = macro_def.parameters[param_idx]->Cast<ColumnRefExpression>().GetColumnName();
		if (named_arguments.find(param_name) != named_arguments.end()) {
			continue; // The user has supplied an argument for this parameter
		}

		const auto it = macro_def.default_parameters.find(param_name);
		D_ASSERT(it != macro_def.default_parameters.end());
		named_arguments[param_name] = it->second->Copy();
	}

	return MacroBindResult(macro_idx);
}

unique_ptr<DummyBinding>
MacroFunction::CreateDummyBinding(const MacroFunction &macro_def, const string &name,
                                  vector<unique_ptr<ParsedExpression>> &positional_arguments,
                                  InsertionOrderPreservingMap<unique_ptr<ParsedExpression>> &named_arguments) {
	// create a MacroBinding to bind this macro's parameters to its arguments
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < positional_arguments.size(); i++) {
		types.emplace_back(LogicalTypeId::UNKNOWN);
		names.push_back(macro_def.parameters[i]->Cast<ColumnRefExpression>().GetColumnName());
	}
	for (auto &kv : named_arguments) {
		types.emplace_back(LogicalTypeId::UNKNOWN);
		names.push_back(kv.first);
		positional_arguments.push_back(std::move(kv.second)); // push defaults into positionals
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
	// In older versions of DuckDB, parameters with defaults were not stored in "parameters", this adds them
	for (auto &kv : default_parameters) {
		bool found = false;
		for (const auto &parameter : parameters) {
			if (StringUtil::CIEquals(kv.first, parameter->Cast<ColumnRefExpression>().GetColumnName())) {
				found = true;
				break;
			}
		}
		if (!found) {
			parameters.push_back(make_uniq<ColumnRefExpression>(kv.first));
		}
	}
}

string MacroFunction::ToSQL() const {
	vector<string> param_strings;
	for (auto &param : parameters) {
		const auto &param_name = param->Cast<ColumnRefExpression>().GetColumnName();
		auto it = default_parameters.find(param_name);
		if (it == default_parameters.end()) {
			param_strings.push_back(param_name);
		} else {
			param_strings.push_back(StringUtil::Format("%s := %s", it->first, it->second->ToString()));
		}
	}
	return StringUtil::Format("(%s) AS ", StringUtil::Join(param_strings, ", "));
}

} // namespace duckdb
