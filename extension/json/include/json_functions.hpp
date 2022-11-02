//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class CastFunctionSet;
struct CastParameters;

class JSONFunctions {
public:
	static vector<CreateScalarFunctionInfo> GetScalarFunctions();
	static vector<CreateTableFunctionInfo> GetTableFunctions();
	static void RegisterCastFunctions(CastFunctionSet &casts, GetCastFunctionInput input);

private:
	// Scalar functions
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
	static CreateScalarFunctionInfo GetContainsFunction();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetValidFunction();

	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}

private:
	// Table functions
	static CreateTableFunctionInfo GetReadJSONObjectsFunction();
};

} // namespace duckdb
