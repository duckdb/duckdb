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
		functions.push_back(GetTypeFunction());
		functions.push_back(GetValidFunction());
		return functions;
	}

private:
	static CreateScalarFunctionInfo GetArrayLengthFunction();
	static vector<CreateScalarFunctionInfo> GetExtractFunctions();
	static CreateScalarFunctionInfo GetTypeFunction();
	static CreateScalarFunctionInfo GetValidFunction();
};

} // namespace duckdb
