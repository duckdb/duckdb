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
		functions.push_back(GetArrayFunction());
		functions.push_back(GetArrayLengthFunction());
		functions.push_back(GetArrayToJSONFunction());
		functions.push_back(GetExtractFunction());
		functions.push_back(GetExtractPathFunction());
		functions.push_back(GetExtractPathTextFunction());
		functions.push_back(GetExtractStringFunction());
		functions.push_back(GetExtractStringOperator());
		functions.push_back(GetObjectFunction());
		functions.push_back(GetQuoteFunction());
		functions.push_back(GetRowToJSONFunction());
		functions.push_back(GetStructureFunction());
		functions.push_back(GetToJSONFunction());
		functions.push_back(GetTypeFunction());
		functions.push_back(GetTransformFunction());
		functions.push_back(GetTransformStrictFunction());
		functions.push_back(GetValidFunction());
		return functions;
	}

private:
	static CreateScalarFunctionInfo GetArrayFunction();
	static CreateScalarFunctionInfo GetArrayLengthFunction();
	static CreateScalarFunctionInfo GetArrayToJSONFunction();
	static CreateScalarFunctionInfo GetExtractFunction();
	static CreateScalarFunctionInfo GetExtractPathFunction();
	static CreateScalarFunctionInfo GetExtractPathTextFunction();
	static CreateScalarFunctionInfo GetExtractStringFunction();
	static CreateScalarFunctionInfo GetExtractStringOperator();
	static CreateScalarFunctionInfo GetObjectFunction();
	static CreateScalarFunctionInfo GetQuoteFunction();
	static CreateScalarFunctionInfo GetRowToJSONFunction();
	static CreateScalarFunctionInfo GetStructureFunction();
	static CreateScalarFunctionInfo GetToJSONFunction();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetTransformFunction();
	static CreateScalarFunctionInfo GetTransformStrictFunction();
	static CreateScalarFunctionInfo GetValidFunction();
};

} // namespace duckdb
