//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

class JSONFunctions {
public:
	static vector<CreateScalarFunctionInfo> GetFunctions() {
		vector<CreateScalarFunctionInfo> functions;

		// Extract functions
		AddAliases({"json_extract", "json_extract_path"}, GetExtractFunction(), functions);
		AddAliases({"json_extract_string", "json_extract_path_text", "->>"}, GetExtractStringFunction(), functions);

		// Create functions
		functions.push_back(GetArrayFunction());
		functions.push_back(GetObjectFunction());
		AddAliases({"to_json", "json_quote"}, GetToJSONFunction(), functions);
		functions.push_back(GetArrayToJSONFunction());
		functions.push_back(GetRowToJSONFunction());
		functions.push_back(GetMergePatchFunction());

		// Structure/Transform
		functions.push_back(GetStructureFunction());
		AddAliases({"json_transform", "from_json"}, GetTransformFunction(), functions);
		AddAliases({"json_transform_strict", "from_json_strict"}, GetTransformStrictFunction(), functions);

		// Other
		functions.push_back(GetArrayLengthFunction());
		functions.push_back(GetTypeFunction());
		functions.push_back(GetValidFunction());

		return functions;
	}

private:
	static CreateScalarFunctionInfo GetExtractFunction();
	static CreateScalarFunctionInfo GetExtractStringFunction();

	static CreateScalarFunctionInfo GetArrayFunction();
	static CreateScalarFunctionInfo GetObjectFunction();
	static CreateScalarFunctionInfo GetToJSONFunction();
	static CreateScalarFunctionInfo GetArrayToJSONFunction();
	static CreateScalarFunctionInfo GetRowToJSONFunction();
	static CreateScalarFunctionInfo GetMergePatchFunction();

	static CreateScalarFunctionInfo GetStructureFunction();
	static CreateScalarFunctionInfo GetTransformFunction();
	static CreateScalarFunctionInfo GetTransformStrictFunction();

	static CreateScalarFunctionInfo GetArrayLengthFunction();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetValidFunction();

	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}
};

} // namespace duckdb
