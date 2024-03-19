//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/double_cast_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "fast_float/fast_float.h"

namespace duckdb {
template <class T>
static bool TryDoubleCast(const char *buf, idx_t len, T &result, bool strict, char decimal_separator = '.') {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	if (*buf == '+') {
		if (strict) {
			// plus is not allowed in strict mode
			return false;
		}
		buf++;
		len--;
	}
	if (strict && len >= 2) {
		if (buf[0] == '0' && StringUtil::CharacterIsDigit(buf[1])) {
			// leading zeros are not allowed in strict mode
			return false;
		}
	}
	auto endptr = buf + len;
	auto parse_result = duckdb_fast_float::from_chars(buf, buf + len, result, decimal_separator);
	if (parse_result.ec != std::errc()) {
		return false;
	}
	auto current_end = parse_result.ptr;
	if (!strict) {
		while (current_end < endptr && StringUtil::CharacterIsSpace(*current_end)) {
			current_end++;
		}
	}
	return current_end == endptr;
}
} // namespace duckdb
