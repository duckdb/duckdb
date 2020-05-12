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
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UpperFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StripAccentsFun {
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
	template <class TA, class TR> static inline TR Length(TA input) {
		auto input_data = input.GetData();
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
};

struct LikeEscapeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LpadFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LtrimFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct LeftFun {
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
	static string_t substring_ascii_only(Vector &result, const char *input_data, int offset, int length);
	static string_t substring_scalar_function(Vector &result, string_t input, int offset, int length,
	                                          unique_ptr<char[]> &output, idx_t &current_len);
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

struct RtrimFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SuffixFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ContainsFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnicodeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RegexpMatchesBindData : public FunctionData {
	RegexpMatchesBindData(std::unique_ptr<re2::RE2> constant_pattern, string range_min, string range_max,
	                      bool range_success);
	~RegexpMatchesBindData();

	std::unique_ptr<re2::RE2> constant_pattern;
	string range_min, range_max;
	bool range_success;

	unique_ptr<FunctionData> Copy() override;
};

} // namespace duckdb
