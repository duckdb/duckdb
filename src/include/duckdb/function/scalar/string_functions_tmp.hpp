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

struct LikeFun {
	static ScalarFunction GetLikeFunction();
	static void RegisterFunction(BuiltinFunctions &set);
	DUCKDB_API static bool Glob(const char *s, idx_t slen, const char *pattern, idx_t plen,
	                            bool allow_question_mark = true);
};

struct LikeEscapeFun {
	static ScalarFunction GetLikeEscapeFun();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SubstringFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static string_t SubstringUnicode(Vector &result, string_t input, int64_t offset, int64_t length);
	static string_t SubstringGrapheme(Vector &result, string_t input, int64_t offset, int64_t length);
};

struct RegexpFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringSplitFun {
	static ScalarFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegexpEscapeFun {
	static ScalarFunction GetFunction();

	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringSplitRegexFun {
	static ScalarFunctionSet GetFunctions();

	static void RegisterFunction(BuiltinFunctions &set);
};

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
