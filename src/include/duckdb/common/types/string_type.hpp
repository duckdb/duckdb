//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

struct string_t {
	friend struct StringComparisonOperators;
	friend class StringSegment;
public:
	static constexpr index_t PREFIX_LENGTH = 4 * sizeof(char);
	static constexpr index_t INLINE_LENGTH = 12;

	string_t() = default;
	string_t(uint32_t len) : length(len) {
		memset(prefix, 0, PREFIX_LENGTH);
		value_.data = nullptr;
	}
	string_t(const char *data, uint32_t len) : length(len) {
		assert(data || length == 0);
		if (IsInlined()) {
			if (length == 0) {
				return;
			}
			// small string: inlined
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(prefix, 0, PREFIX_LENGTH);
			memcpy(prefix, data, length);
			prefix[length] = '\0';
		} else {
			// large string: store pointer
			memcpy(prefix, data, PREFIX_LENGTH);
			value_.data = (char*) data;
		}
	}
	string_t(const char *data) : string_t(data, strlen(data)) {}
	string_t(const string &value) : string_t(value.c_str(), value.size()) {}

	bool IsInlined() const {
		return length < INLINE_LENGTH;
	}

	char *GetData() {
		return IsInlined() ? (char*) prefix : value_.data;
	}

	const char *GetData() const {
		return IsInlined() ? (const char*) prefix : value_.data;
	}

	index_t GetSize() const {
		return length;
	}

	string GetString() const {
		return string(GetData(), GetSize());
	}
private:
	uint32_t length;
	char prefix[4];
	union {
		char inlined[8];
		char *data;
	} value_;
};

}; // namespace duckdb