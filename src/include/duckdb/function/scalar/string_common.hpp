#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

idx_t LowerLength(const char *input_data, idx_t input_length);
void LowerCase(const char *input_data, idx_t input_length, char *result_data);
idx_t FindStrInStr(const string_t &haystack_s, const string_t &needle_s);
idx_t FindStrInStr(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle, idx_t needle_size);

ScalarFunction GetStringContains();

static inline bool IsCharacter(char c) {
	return (c & 0xc0) != 0x80;
}

} // namespace duckdb
