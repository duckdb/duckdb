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
		functions.push_back(GetTypeFunction());
		return functions;
	}

private:
	static vector<CreateScalarFunctionInfo> GetExtractFunctions();
	static CreateScalarFunctionInfo GetTypeFunction();
};

} // namespace duckdb
