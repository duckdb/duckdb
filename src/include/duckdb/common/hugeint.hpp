#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>

namespace duckdb {

struct hugeint_t {
public:
	uint64_t lower;
	int64_t upper;

public:
	DUCKDB_API hugeint_t() = default;
	DUCKDB_API hugeint_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
	DUCKDB_API constexpr hugeint_t(int64_t upper, uint64_t lower) : lower(lower), upper(upper) {
	}
	DUCKDB_API constexpr hugeint_t(const hugeint_t &rhs) = default;
	DUCKDB_API constexpr hugeint_t(hugeint_t &&rhs) = default;
	DUCKDB_API hugeint_t &operator=(const hugeint_t &rhs) = default;
	DUCKDB_API hugeint_t &operator=(hugeint_t &&rhs) = default;

	DUCKDB_API string ToString() const;

	// comparison operators
	DUCKDB_API bool operator==(const hugeint_t &rhs) const;
	DUCKDB_API bool operator!=(const hugeint_t &rhs) const;
	DUCKDB_API bool operator<=(const hugeint_t &rhs) const;
	DUCKDB_API bool operator<(const hugeint_t &rhs) const;
	DUCKDB_API bool operator>(const hugeint_t &rhs) const;
	DUCKDB_API bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	DUCKDB_API hugeint_t operator+(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator-(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator*(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator/(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator%(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator-() const;

	// bitwise operators
	DUCKDB_API hugeint_t operator>>(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator<<(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator&(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator|(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator^(const hugeint_t &rhs) const;
	DUCKDB_API hugeint_t operator~() const;

	// in-place operators
	DUCKDB_API hugeint_t &operator+=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator-=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator*=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator/=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator%=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator>>=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator<<=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator&=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator|=(const hugeint_t &rhs);
	DUCKDB_API hugeint_t &operator^=(const hugeint_t &rhs);
};

} // namespace duckdb
