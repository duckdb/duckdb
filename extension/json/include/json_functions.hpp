//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class JSONFunctions {
public:
	static vector<CreateScalarFunctionInfo> GetFunctions() {
		vector<CreateScalarFunctionInfo> functions;
		functions.push_back(GetArrayFunction());
		functions.push_back(GetArrayLengthFunction());
		functions.push_back(GetExtractFunction());
		functions.push_back(GetJSONFunction());
		functions.push_back(GetObjectFunction());
		functions.push_back(GetStructureFunction());
		functions.push_back(GetTypeFunction());
		functions.push_back(GetTransformFunction());
		functions.push_back(GetTransformStrictFunction());
		functions.push_back(GetValidFunction());
		return functions;
	}

private:
	static CreateScalarFunctionInfo GetArrayFunction();
	static CreateScalarFunctionInfo GetArrayLengthFunction();
	static CreateScalarFunctionInfo GetExtractFunction();
	static CreateScalarFunctionInfo GetJSONFunction();
	static CreateScalarFunctionInfo GetObjectFunction();
	static CreateScalarFunctionInfo GetStructureFunction();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetTransformFunction();
	static CreateScalarFunctionInfo GetTransformStrictFunction();
	static CreateScalarFunctionInfo GetValidFunction();
};

} // namespace duckdb
