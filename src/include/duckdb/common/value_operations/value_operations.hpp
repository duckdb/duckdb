//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/value_operations/value_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ValueOperations {
	//===--------------------------------------------------------------------===//
	// Numeric Operations
	//===--------------------------------------------------------------------===//
	// A + B
	static Value Add(const Value &left, const Value &right);
	// A - B
	static Value Subtract(const Value &left, const Value &right);
	// A * B
	static Value Multiply(const Value &left, const Value &right);
	// A / B
	static Value Divide(const Value &left, const Value &right);
	// A % B
	static Value Modulo(const Value &left, const Value &right);
	// // MIN(A, B)
	// static Value Min(const Value &left, const Value &right);
	// // MAX(A, B)
	// static Value Max(const Value &left, const Value &right);
	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// A == B
	static bool Equals(const Value &left, const Value &right);
	// A != B
	static bool NotEquals(const Value &left, const Value &right);
	// A > B
	static bool GreaterThan(const Value &left, const Value &right);
	// A >= B
	static bool GreaterThanEquals(const Value &left, const Value &right);
	// A < B
	static bool LessThan(const Value &left, const Value &right);
	// A <= B
	static bool LessThanEquals(const Value &left, const Value &right);
	//===--------------------------------------------------------------------===//
	// Hash functions
	//===--------------------------------------------------------------------===//
	// result = HASH(A)
	static hash_t Hash(const Value &left);
};
} // namespace duckdb
