#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>

namespace duckdb {

struct hugeint_t;

struct uhugeint_t {
public:
	uint64_t lower;
	uint64_t upper;

public:
	uhugeint_t() = default;
	DUCKDB_API uhugeint_t(uint64_t value); // NOLINT: Allow implicit conversion from `uint64_t`
	DUCKDB_API explicit uhugeint_t(const hugeint_t &value);
	constexpr uhugeint_t(uint64_t upper, uint64_t lower) : lower(lower), upper(upper) {
	}
	constexpr uhugeint_t(const uhugeint_t &rhs) = default;
	constexpr uhugeint_t(uhugeint_t &&rhs) = default;
	uhugeint_t &operator=(const uhugeint_t &rhs) = default;
	uhugeint_t &operator=(uhugeint_t &&rhs) = default;

	DUCKDB_API string ToString() const;

	// comparison operators
	DUCKDB_API bool operator==(const uhugeint_t &rhs) const;
	DUCKDB_API bool operator!=(const uhugeint_t &rhs) const;
	DUCKDB_API bool operator<=(const uhugeint_t &rhs) const;
	DUCKDB_API bool operator<(const uhugeint_t &rhs) const;
	DUCKDB_API bool operator>(const uhugeint_t &rhs) const;
	DUCKDB_API bool operator>=(const uhugeint_t &rhs) const;

	// arithmetic operators
	DUCKDB_API uhugeint_t operator+(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator-(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator*(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator/(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator%(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator-() const;

	// bitwise operators
	DUCKDB_API uhugeint_t operator>>(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator<<(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator&(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator|(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator^(const uhugeint_t &rhs) const;
	DUCKDB_API uhugeint_t operator~() const;

	// in-place operators
	DUCKDB_API uhugeint_t &operator+=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator-=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator*=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator/=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator%=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator>>=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator<<=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator&=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator|=(const uhugeint_t &rhs);
	DUCKDB_API uhugeint_t &operator^=(const uhugeint_t &rhs);

};

} // namespace duckdb
