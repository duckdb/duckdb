//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/ubigint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/multiply.hpp"

namespace duckdb {

struct ubigint_t { // NOLINT
public:
	uint64_t value;

	ubigint_t() : value(0) {
	}
	ubigint_t(uint64_t value) : value(value) { // NOLINT: allow implicit conversion
	}

	// Comparison operators
	inline bool operator==(const ubigint_t &rhs) const {
		return value == rhs.value;
	}
	inline bool operator!=(const ubigint_t &rhs) const {
		return value != rhs.value;
	}
	inline bool operator<(const ubigint_t &rhs) const {
		return value < rhs.value;
	}
	inline bool operator<=(const ubigint_t &rhs) const {
		return value <= rhs.value;
	}
	inline bool operator>(const ubigint_t &rhs) const {
		return value > rhs.value;
	}
	inline bool operator>=(const ubigint_t &rhs) const {
		return value >= rhs.value;
	}

	// Checked arithmetic operators
	inline ubigint_t operator+(const ubigint_t &rhs) const {
		return ubigint_t(AddOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, rhs.value));
	}
	inline ubigint_t operator-(const ubigint_t &rhs) const {
		return ubigint_t(SubtractOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, rhs.value));
	}
	inline ubigint_t operator*(const ubigint_t &rhs) const {
		return ubigint_t(MultiplyOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, rhs.value));
	}
	inline ubigint_t operator/(const ubigint_t &rhs) const {
		if (rhs.value == 0) {
			throw OutOfRangeException("Division by zero!");
		}
		return ubigint_t(value / rhs.value);
	}
	inline ubigint_t operator%(const ubigint_t &rhs) const {
		if (rhs.value == 0) {
			throw OutOfRangeException("Modulo by zero!");
		}
		return ubigint_t(value % rhs.value);
	}

	// Compound assignment operators
	inline ubigint_t &operator+=(const ubigint_t &rhs) {
		value = AddOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, rhs.value);
		return *this;
	}
	inline ubigint_t &operator-=(const ubigint_t &rhs) {
		value = SubtractOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, rhs.value);
		return *this;
	}
	inline ubigint_t &operator*=(const ubigint_t &rhs) {
		value = MultiplyOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, rhs.value);
		return *this;
	}
	inline ubigint_t &operator/=(const ubigint_t &rhs) {
		if (rhs.value == 0) {
			throw OutOfRangeException("Division by zero!");
		}
		value /= rhs.value;
		return *this;
	}
	inline ubigint_t &operator%=(const ubigint_t &rhs) {
		if (rhs.value == 0) {
			throw OutOfRangeException("Modulo by zero!");
		}
		value %= rhs.value;
		return *this;
	}

	// Pre/post increment/decrement
	inline ubigint_t &operator++() {
		value = AddOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, 1);
		return *this;
	}
	inline ubigint_t operator++(int) {
		ubigint_t result = *this;
		value = AddOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, 1);
		return result;
	}
	inline ubigint_t &operator--() {
		value = SubtractOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, 1);
		return *this;
	}
	inline ubigint_t operator--(int) {
		ubigint_t result = *this;
		value = SubtractOperatorOverflowCheck::Operation<uint64_t, uint64_t, uint64_t>(value, 1);
		return result;
	}
};

} // namespace duckdb
