//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/string_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "utf8proc.hpp"

namespace re2 {
class RE2;
}

namespace duckdb {

struct ReverseFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LowerFun {
	static uint8_t ascii_to_lower_map[];

	//! Returns the length of the result string obtained from lowercasing the given input (in bytes)
	static idx_t LowerLength(const char *input_data, idx_t input_length);
	//! Lowercases the string to the target output location, result_data must have space for at least LowerLength bytes
	static void LowerCase(const char *input_data, idx_t input_length, char *result_data);

	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UpperFun {
	static uint8_t ascii_to_upper_map[];

	static void RegisterFunction(BuiltinFunctions &set);
};

struct StripAccentsFun {
	static bool IsAscii(const char *input, idx_t n);
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ConcatFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ConcatWSFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LengthFun {
	static void RegisterFunction(BuiltinFunctions &set);
	template <class TA, class TR>
	static inline TR Length(TA input) {
		auto input_data = input.GetDataUnsafe();
		auto input_length = input.GetSize();
		for (idx_t i = 0; i < input_length; i++) {
			if (input_data[i] & 0x80) {
				int64_t length = 0;
				// non-ascii character: use grapheme iterator on remainder of string
				utf8proc_grapheme_callback(input_data, input_length, [&](size_t start, size_t end) {
					length++;
					return true;
				});
				return length;
			}
		}
		return input_length;
	}
};

struct LikeFun {
	static void RegisterFunction(BuiltinFunctions &set);
	DUCKDB_API static bool Glob(const char *s, idx_t slen, const char *pattern, idx_t plen);
};

struct LikeEscapeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LpadFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LeftFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MD5Fun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct NFCNormalizeFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RightFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegexpFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SubstringFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static string_t SubstringScalarFunction(Vector &result, string_t input, int64_t offset, int64_t length);
};

struct PrintfFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct InstrFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PrefixFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReplaceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RpadFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SuffixFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct TrimFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ContainsFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
	static idx_t Find(const string_t &haystack, const string_t &needle);
	static idx_t Find(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
	                  idx_t needle_size);
};

struct UnicodeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StringSplitFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ASCII {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CHR {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MismatchesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LevenshteinFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct JaccardFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct JaroWinklerFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
