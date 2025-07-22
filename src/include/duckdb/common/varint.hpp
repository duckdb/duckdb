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
	string value;

public:
	DUCKDB_API varint_t(int64_t value);
	// DUCKDB_API varint_t(hugeint_t value);
	// DUCKDB_API varint_t(uhugeint_t value);
	// DUCKDB_API varint_t(string_t value);
	
	varint_t() = default;
	explicit varint_t(string value) : value(std::move(value)) {
	}
	varint_t(varint_t&& other) noexcept {
		value = std::move(other.value);
	}
	varint_t(const varint_t &rhs) = default;

	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs)  noexcept {
		if (this != &rhs) {
		value = std::move(rhs.value);
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