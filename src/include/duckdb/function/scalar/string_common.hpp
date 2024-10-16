#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

bool IsAscii(const char *input, idx_t n);
idx_t LowerLength(const char *input_data, idx_t input_length);
void LowerCase(const char *input_data, idx_t input_length, char *result_data);
idx_t FindStrInStr(const string_t &haystack_s, const string_t &needle_s);
idx_t FindStrInStr(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle, idx_t needle_size);
string_t SubstringASCII(Vector &result, string_t input, int64_t offset, int64_t length);
string_t SubstringUnicode(Vector &result, string_t input, int64_t offset, int64_t length);
string_t SubstringGrapheme(Vector &result, string_t input, int64_t offset, int64_t length);

ScalarFunction GetStringContains();
DUCKDB_API bool Glob(const char *s, idx_t slen, const char *pattern, idx_t plen, bool allow_question_mark = true);

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

} // namespace duckdb
