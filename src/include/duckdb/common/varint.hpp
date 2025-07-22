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
	string_t value;

public:
	DUCKDB_API varint_t(int64_t input_value);
	// DUCKDB_API varint_t(hugeint_t value);
	// DUCKDB_API varint_t(uhugeint_t value);
	// DUCKDB_API varint_t(string_t value);

	varint_t() = default;

	// explicit varint_t(const char *data, size_t len) : value(nullptr), size(len) {
	// 	if (len > 0) {
	// 		value = make_uniq_array<char>(len);
	// 		memcpy(value.get(), data, len);
	// 	}
	// }

	varint_t(const varint_t &rhs) = default;
	varint_t(varint_t &&other) = default;
	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs) = default;

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
