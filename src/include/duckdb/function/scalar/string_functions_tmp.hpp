//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/string_functions_tmp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb_re2 {
class RE2;
}

namespace duckdb {

struct MD5Fun {
	static ScalarFunctionSet GetFunctions();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct MD5NumberFun {
	static ScalarFunctionSet GetFunctions();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct SHA1Fun {
	static ScalarFunctionSet GetFunctions();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct SHA256Fun {
	static ScalarFunctionSet GetFunctions();

	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
