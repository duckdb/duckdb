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
	static constexpr index_t PREFIX_LENGTH = 4 * sizeof(char);

	string_t() = default;
	string_t(uint32_t len) : length(len) {
		dataptr = nullptr;
	}
	string_t(const char *data, uint32_t len) : length(len) {
		assert(data || length == 0);
		if (length == 0) {
			memset(prefix, 0, PREFIX_LENGTH);
		} else if (length < PREFIX_LENGTH) {
			memcpy(prefix, data, length);
			memset(prefix + length, 0, PREFIX_LENGTH - length);
		} else {
			memcpy(prefix, data, PREFIX_LENGTH);
		}
		dataptr = (char*) data;
	}
	string_t(const char *data) : string_t(data, strlen(data)) {}
	string_t(const string &value) : string_t(value.c_str(), value.size()) {}

	char *GetData() {
		return dataptr;
	}

	const char *GetData() const {
		return dataptr;
	}

	index_t GetSize() const {
		return length;
	}

	string GetString() const {
		return string(GetData(), GetSize());
	}

	void Finalize() {
		if (length < PREFIX_LENGTH) {
			memcpy(prefix, dataptr, length);
			memset(prefix + length, 0, PREFIX_LENGTH - length);
		} else {
			// copy the data into the prefix
			memcpy(prefix, dataptr, PREFIX_LENGTH);
		}
		// set trailing NULL byte
		dataptr[length] = '\0';
	}

	void Verify();
private:
	uint32_t length;
	char prefix[4];
	char *dataptr;
};

}; // namespace duckdb
