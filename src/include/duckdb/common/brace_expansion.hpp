//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/brace_expansion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

class BraceExpansion {
public:
	static vector<string> brace_expansion(const string &pattern);
	static bool has_brace_expansion(const string &pattern);
};

} // namespace duckdb
