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

struct StripAccentsFun {
	static bool IsAscii(const char *input, idx_t n);
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LengthFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static inline bool IsCharacter(char c) {
		return (c & 0xc0) != 0x80;
	}

	template <class TA, class TR>
	static inline TR Length(TA input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		TR length = 0;
		for (idx_t i = 0; i < input_length; i++) {
			length += IsCharacter(input_data[i]);
		}
		return length;
	}

	template <class TA, class TR>
	static inline TR GraphemeCount(TA input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		for (idx_t i = 0; i < input_length; i++) {
			if (input_data[i] & 0x80) {
				// non-ascii character: use grapheme iterator on remainder of string
				return UnsafeNumericCast<TR>(Utf8Proc::GraphemeCount(input_data, input_length));
			}
		}
		return UnsafeNumericCast<TR>(input_length);
	}
};

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

struct NFCNormalizeFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SubstringFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static string_t SubstringUnicode(Vector &result, string_t input, int64_t offset, int64_t length);
	static string_t SubstringGrapheme(Vector &result, string_t input, int64_t offset, int64_t length);
};

struct PrefixFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SuffixFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ContainsFun {
	static ScalarFunctionSet GetFunctions();
	static ScalarFunction GetStringContains();
	static void RegisterFunction(BuiltinFunctions &set);
	static idx_t Find(const string_t &haystack, const string_t &needle);
	static idx_t Find(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
	                  idx_t needle_size);
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
