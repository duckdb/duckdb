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
		auto functions = GetExtractFunctions();
		functions.push_back(GetArrayLengthFunction());
		functions.push_back(GetArrayFunction());
		functions.push_back(GetObjectFunction());
		functions.push_back(GetJSONFunction());
		functions.push_back(GetTypeFunction());
		functions.push_back(GetStructureFunction());
		functions.push_back(GetValidFunction());
		return functions;
	}

private:
	static CreateScalarFunctionInfo GetArrayLengthFunction();
	static vector<CreateScalarFunctionInfo> GetExtractFunctions();
	static CreateScalarFunctionInfo GetArrayFunction();
	static CreateScalarFunctionInfo GetObjectFunction();
	static CreateScalarFunctionInfo GetJSONFunction();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetStructureFunction();
	static CreateScalarFunctionInfo GetValidFunction();
};

} // namespace duckdb
