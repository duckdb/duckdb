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

// #include "duckdb/common/typedefs.hpp"
// #include "duckdb/common/types/string_type.hpp"

namespace duckdb {

struct varint_t { // NOLINT: use numeric casing
public:
	string value;

public:
	DUCKDB_API varint_t(int64_t value);
	DUCKDB_API varint_t(hugeint_t value);
	DUCKDB_API varint_t(uhugeint_t value);
	DUCKDB_API varint_t(string_t value);

	varint_t() = default;
	explicit varint_t(string value) : value(std::move(value)) {
	}
	varint_t(const varint_t &rhs) = default;
	varint_t(varint_t &&rhs) = default;
	varint_t &operator=(const varint_t &rhs) = default;
	varint_t &operator=(varint_t &&rhs) = default;

	DUCKDB_API string ToString() const;

	// comparison operators
	DUCKDB_API bool operator==(const varint_t &rhs) const;
	DUCKDB_API bool operator!=(const varint_t &rhs) const;
	DUCKDB_API bool operator<=(const varint_t &rhs) const;
	DUCKDB_API bool operator<(const varint_t &rhs) const;
	DUCKDB_API bool operator>(const varint_t &rhs) const;
	DUCKDB_API bool operator>=(const varint_t &rhs) const;

	// // arithmetic operators
	DUCKDB_API varint_t operator+(const varint_t &rhs) const;
	DUCKDB_API varint_t operator-(const varint_t &rhs) const;
	DUCKDB_API varint_t operator*(const varint_t &rhs) const;
	DUCKDB_API varint_t operator/(const varint_t &rhs) const;
	DUCKDB_API varint_t operator%(const varint_t &rhs) const;
	DUCKDB_API varint_t operator-() const;

	// bitwise operators
	DUCKDB_API varint_t operator>>(const varint_t &rhs) const;
	DUCKDB_API varint_t operator<<(const varint_t &rhs) const;
	DUCKDB_API varint_t operator&(const varint_t &rhs) const;
	DUCKDB_API varint_t operator|(const varint_t &rhs) const;
	DUCKDB_API varint_t operator^(const varint_t &rhs) const;
	DUCKDB_API varint_t operator~() const;

	// in-place operators
	DUCKDB_API varint_t &operator+=(const varint_t &rhs);
	DUCKDB_API varint_t &operator-=(const varint_t &rhs);
	DUCKDB_API varint_t &operator*=(const varint_t &rhs);
	DUCKDB_API varint_t &operator/=(const varint_t &rhs);
	DUCKDB_API varint_t &operator%=(const varint_t &rhs);
	DUCKDB_API varint_t &operator>>=(const varint_t &rhs);
	DUCKDB_API varint_t &operator<<=(const varint_t &rhs);
	DUCKDB_API varint_t &operator&=(const varint_t &rhs);
	DUCKDB_API varint_t &operator|=(const varint_t &rhs);
	DUCKDB_API varint_t &operator^=(const varint_t &rhs);

	// boolean operators
	DUCKDB_API explicit operator bool() const;
	DUCKDB_API bool operator!() const;

	// cast operators
	DUCKDB_API explicit operator uint8_t() const;
	DUCKDB_API explicit operator uint16_t() const;
	DUCKDB_API explicit operator uint32_t() const;
	DUCKDB_API explicit operator uint64_t() const;
	DUCKDB_API explicit operator int8_t() const;
	DUCKDB_API explicit operator int16_t() const;
	DUCKDB_API explicit operator int32_t() const;
	DUCKDB_API explicit operator int64_t() const;
	DUCKDB_API operator uhugeint_t() const; // NOLINT: Allow implicit conversion from `hugeint_t`
};

} // namespace duckdb