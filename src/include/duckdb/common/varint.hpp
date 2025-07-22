//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/varint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>

#include "hugeint.hpp"

namespace duckdb {

struct varint_t { // NOLINT: use numeric casing
public:
	std::unique_ptr<char[]> value;
	size_t size = 0;

public:
	DUCKDB_API varint_t(int64_t input_value);
	// DUCKDB_API varint_t(hugeint_t value);
	// DUCKDB_API varint_t(uhugeint_t value);
	// DUCKDB_API varint_t(string_t value);

	varint_t() = default;

	explicit varint_t(const char *data, size_t len) : value(nullptr), size(len) {
		if (len > 0) {
			value = make_uniq_array<char>(len);
			memcpy(value.get(), data, len);
		}
	}

	varint_t(const varint_t &rhs) : value(nullptr), size(rhs.size) {
		if (size > 0) {
			value = make_uniq_array<char>(size);
			memcpy(value.get(), rhs.value.get(), size);
		}
	}

	varint_t(varint_t &&other) noexcept : value(std::move(other.value)), size(other.size) {
		other.size = 0;
	}

	varint_t &operator=(const varint_t &rhs) {
		if (this != &rhs) {
			size = rhs.size;
			if (size > 0) {
				value = make_uniq_array<char>(size);
				memcpy(value.get(), rhs.value.get(), size);
			} else {
				value.reset();
			}
		}
		return *this;
	}

	varint_t &operator=(varint_t &&rhs) noexcept {
		if (this != &rhs) {
			value = std::move(rhs.value);
			size = rhs.size;
			rhs.size = 0;
		}
		return *this;
	}

	// DUCKDB_API string ToString() const;

	// comparison operators
	DUCKDB_API bool operator==(const varint_t &rhs) const;
	DUCKDB_API bool operator!=(const varint_t &rhs) const;
	DUCKDB_API bool operator<=(const varint_t &rhs) const;
	DUCKDB_API bool operator<(const varint_t &rhs) const;
	DUCKDB_API bool operator>(const varint_t &rhs) const;
	DUCKDB_API bool operator>=(const varint_t &rhs) const;

	// // arithmetic operators
	DUCKDB_API varint_t operator*(const varint_t &rhs) const;

	// in-place operators
	DUCKDB_API varint_t &operator+=(const varint_t &rhs);

};

} // namespace duckdb