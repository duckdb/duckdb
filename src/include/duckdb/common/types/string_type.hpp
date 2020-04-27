//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include <cstring>
#include <cassert>

namespace duckdb {

struct string_t {
	friend struct StringComparisonOperators;
	friend class StringSegment;

public:
	static constexpr idx_t PREFIX_LENGTH = 4 * sizeof(char);
	static constexpr idx_t INLINE_LENGTH = 12;

	string_t() = default;
	string_t(uint32_t len) : length(len) {
		memset(prefix, 0, PREFIX_LENGTH);
		value_.data = nullptr;
	}
	string_t(const char *data, uint32_t len) : length(len) {
		assert(data || length == 0);
		if (IsInlined()) {
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(prefix, 0, PREFIX_LENGTH);
			if (length == 0) {
				return;
			}
			// small string: inlined
			memcpy(prefix, data, length);
			prefix[length] = '\0';
		} else {
			// large string: store pointer
			memcpy(prefix, data, PREFIX_LENGTH);
			value_.data = (char *)data;
		}
	}
	string_t(const char *data) : string_t(data, strlen(data)) {
	}
	string_t(const string &value) : string_t(value.c_str(), value.size()) {
	}

	bool IsInlined() const {
		return length < INLINE_LENGTH;
	}

	char *GetData() {
		return IsInlined() ? (char *)prefix : value_.data;
	}

	const char *GetData() const {
		return IsInlined() ? (const char *)prefix : value_.data;
	}

	const char *GetPrefix() const {
		return prefix;
	}

	idx_t GetSize() const {
		return length;
	}

	string GetString() const {
		return string(GetData(), GetSize());
	}

	void Finalize() {
		// set trailing NULL byte
		auto dataptr = (char *)GetData();
		dataptr[length] = '\0';
		if (length < INLINE_LENGTH) {
			// fill prefix with zeros if the length is smaller than the prefix length
			for (idx_t i = length; i < PREFIX_LENGTH; i++) {
				prefix[i] = '\0';
			}
		} else {
			// copy the data into the prefix
			memcpy(prefix, dataptr, PREFIX_LENGTH);
		}
	}

	void Verify();

private:
	uint32_t length;
	char prefix[4];
	union {
		char inlined[8];
		char *data;
	} value_;
};

}; // namespace duckdb
