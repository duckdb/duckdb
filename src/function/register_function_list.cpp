#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_list.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

static LogicalType GetLogicalType(string &logical_type_string) {
	LogicalType type;
	if (StringUtil::Contains(logical_type_string, "ANY")) {
		if (StringUtil::CIEquals(logical_type_string, "ANY")) {
			type = LogicalType::ANY;
		} else if (StringUtil::CIEquals(logical_type_string, "ANY[]")) {
			type = LogicalType::LIST(LogicalType::ANY);
		} else if (StringUtil::CIEquals(logical_type_string, "ANY[][]")) {
			type = LogicalType::LIST(LogicalType::LIST(LogicalType::ANY));
		} else if (StringUtil::CIEquals(logical_type_string, "MAP(ANY,ANY)")) {
			type = LogicalType::MAP(LogicalType::ANY, LogicalType::ANY);
		} else {
			throw InternalException("Unsupported type: '%s'", logical_type_string);
		}
	} else {
		type = Parser::ParseLogicalType(logical_type_string);
	}
	if (type == LogicalType::INVALID) {
		throw InternalException("Failed to convert string '%s' to LogicalType", logical_type_string);
	}
	return type;
}

static void FillFunctionParameters(FunctionDescription &function_description, const char *function_name,
                                   vector<string> &parameters, vector<string> &descriptions, vector<string> &examples) {
	for (string &parameter : parameters) {
		vector<string> parameter_name_type = StringUtil::Split(parameter, "::");
		if (parameter_name_type.size() == 1) {
			function_description.parameter_names.push_back(std::move(parameter_name_type[0]));
			function_description.parameter_types.push_back(LogicalType::ANY);
		} else if (parameter_name_type.size() == 2) {
			function_description.parameter_names.push_back(std::move(parameter_name_type[0]));
			function_description.parameter_types.push_back(GetLogicalType(parameter_name_type[1]));
		} else {
			throw InternalException("Ill formed function variant for function '%s'", function_name);
		}
	}
}

template <class T>
void FillFunctionDescriptions(const StaticFunctionDefinition &function, T &info) {
	vector<string> variants = StringUtil::Split(function.parameters, '\1');
	vector<string> descriptions = StringUtil::Split(function.description, '\1');
	vector<string> examples = StringUtil::Split(function.example, '\1');

	// add single variant for functions that take no arguments
	if (variants.size() == 0) {
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
		} else if (descriptions.size() != 0) {
			throw InternalException("Incorrect number of function descriptions for function '%s'", function.name);
		}
		// examples
		if (examples.size() == variants.size()) {
			function_description.examples = StringUtil::Split(examples[variant_index], '\2');
		} else if (examples.size() == 1) {
			function_description.examples = StringUtil::Split(examples[0], '\2');
		} else if (examples.size() != 0) {
			throw InternalException("Incorrect number of function examples for function '%s'", function.name);
		}
		info.descriptions.push_back(std::move(function_description));
	}
}

template <class T>
static void FillExtraInfo(const StaticFunctionDefinition &function, T &info) {
	info.internal = true;
	FillFunctionDescriptions(function, info);
}

static void RegisterFunctionList(Catalog &catalog, CatalogTransaction transaction,
                                 const StaticFunctionDefinition *functions) {
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
			catalog.CreateFunction(transaction, info);
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
			catalog.CreateFunction(transaction, info);
		} else {
			throw InternalException("Do not know how to register function of this type");
		}
	}
}

void FunctionList::RegisterFunctions(Catalog &catalog, CatalogTransaction transaction) {
	RegisterFunctionList(catalog, transaction, FunctionList::GetInternalFunctionList());
}

} // namespace duckdb
