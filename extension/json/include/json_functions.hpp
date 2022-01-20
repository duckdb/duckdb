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
	static vector<ScalarFunction> GetFunctions() {
		vector<ScalarFunction> functions;
		for (const auto &extract_fun : GetExtractFunctions()) {
			functions.push_back(extract_fun);
		}
		functions.push_back(GetTypeFunction());
		return functions;
	}

private:
	static vector<ScalarFunction> GetExtractFunctions();
	static ScalarFunction GetTypeFunction();
};

} // namespace duckdb
