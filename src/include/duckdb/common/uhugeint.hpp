#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/common/string.hpp"
#include <stdint.h>

namespace duckdb {

// Forward declaration to allow conversion between hugeint and uhugeint
struct hugeint_t; // NOLINT

struct uhugeint_t { // NOLINT
public:
	uint64_t lower;
	uint64_t upper;

public:
	uhugeint_t() = default;
	DUCKDB_API uhugeint_t(uint64_t value); // NOLINT: Allow implicit conversion from `uint64_t`
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

	// boolean operators
	DUCKDB_API explicit operator bool() const;
	DUCKDB_API bool operator!() const;

	// cast operators -- doesn't check bounds/overflow/underflow
	DUCKDB_API explicit operator uint8_t() const;
	DUCKDB_API explicit operator uint16_t() const;
	DUCKDB_API explicit operator uint32_t() const;
	DUCKDB_API explicit operator uint64_t() const;
	DUCKDB_API explicit operator int8_t() const;
	DUCKDB_API explicit operator int16_t() const;
	DUCKDB_API explicit operator int32_t() const;
	DUCKDB_API explicit operator int64_t() const;
	DUCKDB_API operator hugeint_t() const; // NOLINT: Allow implicit conversion from `uhugeint_t`
};

} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::uhugeint_t> {
	size_t operator()(const duckdb::uhugeint_t &val) const {
		using std::hash;
		return hash<uint64_t> {}(val.upper) ^ hash<uint64_t> {}(val.lower);
	}
};
} // namespace std
