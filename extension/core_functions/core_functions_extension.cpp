#define DUCKDB_EXTENSION_MAIN
#include "core_functions_extension.hpp"

#include "core_functions/function_list.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

static void FillFunctionParameters(FunctionDescription &function_description, const char *function_name,
                                   vector<string> &parameters, vector<string> &descriptions, vector<string> &examples) {
	for (string &parameter : parameters) {
		vector<string> parameter_name_type = StringUtil::Split(parameter, "::");
		if (parameter_name_type.size() == 1) {
			function_description.parameter_names.push_back(std::move(parameter_name_type[0]));
			function_description.parameter_types.push_back(LogicalType::ANY);
		} else if (parameter_name_type.size() == 2) {
			function_description.parameter_names.push_back(std::move(parameter_name_type[0]));
			function_description.parameter_types.push_back(DBConfig::ParseLogicalType(parameter_name_type[1]));
		} else {
			throw InternalException("Ill formed function variant for function '%s'", function_name);
		}
	}
}

template <class T>
static void FillFunctionDescriptions(const StaticFunctionDefinition &function, T &info) {
	vector<string> variants = StringUtil::Split(function.parameters, '\1');
	vector<string> descriptions = StringUtil::Split(function.description, '\1');
	vector<string> examples = StringUtil::Split(function.example, '\1');

	// add single variant for functions that take no arguments
	if (variants.empty()) {
		variants.push_back("");
	}

	for (idx_t variant_index = 0; variant_index < variants.size(); variant_index++) {
		FunctionDescription function_description;
		// parameter_names and parameter_types
		vector<string> parameters = StringUtil::SplitWithParentheses(variants[variant_index], ',');
		FillFunctionParameters(function_description, function.name, parameters, descriptions, examples);
		// description
		if (descriptions.size() == variants.size()) {
			function_description.description = descriptions[variant_index];
		} else if (descriptions.size() == 1) {
			function_description.description = descriptions[0];
		} else if (!descriptions.empty()) {
			throw InternalException("Incorrect number of function descriptions for function '%s'", function.name);
		}
		// examples
		if (examples.size() == variants.size()) {
			function_description.examples = StringUtil::Split(examples[variant_index], '\2');
		} else if (examples.size() == 1) {
			function_description.examples = StringUtil::Split(examples[0], '\2');
		} else if (!examples.empty()) {
			throw InternalException("Incorrect number of function examples for function '%s'", function.name);
		}
		info.descriptions.push_back(std::move(function_description));
	}
}

template <class T>
static void FillExtraInfo(const StaticFunctionDefinition &function, T &info) {
	info.internal = true;
	FillFunctionDescriptions(function, info);
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
}

void LoadInternal(DuckDB &db) {
	auto functions = StaticFunctionDefinition::GetFunctionList();
	for (idx_t i = 0; functions[i].name; i++) {
		auto &function = functions[i];
		if (function.get_function || function.get_function_set) {
			// scalar function
			ScalarFunctionSet result;
			if (function.get_function) {
				result.AddFunction(function.get_function());
			} else {
				result = function.get_function_set();
			}
			result.name = function.name;
			CreateScalarFunctionInfo info(result);
			FillExtraInfo(function, info);
			ExtensionUtil::RegisterFunction(*db.instance, std::move(info));
		} else if (function.get_aggregate_function || function.get_aggregate_function_set) {
			// aggregate function
			AggregateFunctionSet result;
			if (function.get_aggregate_function) {
				result.AddFunction(function.get_aggregate_function());
			} else {
				result = function.get_aggregate_function_set();
			}
			result.name = function.name;
			CreateAggregateFunctionInfo info(result);
			FillExtraInfo(function, info);
			ExtensionUtil::RegisterFunction(*db.instance, std::move(info));
		} else {
			throw InternalException("Do not know how to register function of this type");
		}
	}
}

void CoreFunctionsExtension::Load(DuckDB &db) {
	LoadInternal(db);
}

std::string CoreFunctionsExtension::Name() {
	return "core_functions";
}

std::string CoreFunctionsExtension::Version() const {
#ifdef EXT_VERSION_CORE_FUNCTIONS
	return EXT_VERSION_CORE_FUNCTIONS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void core_functions_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	duckdb::LoadInternal(db_wrapper);
}

DUCKDB_EXTENSION_API const char *core_functions_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
