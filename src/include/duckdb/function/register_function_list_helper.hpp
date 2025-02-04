//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/register_function_list_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
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

template <class FunctionDefinition, class T>
static void FillFunctionDescriptions(const FunctionDefinition &function, T &info) {
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

} // namespace duckdb
