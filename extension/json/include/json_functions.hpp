//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class JSONFunctions {
public:
	static vector<ScalarFunction> GetExtractFunctions();
};

} // namespace duckdb
