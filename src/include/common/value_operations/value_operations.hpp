//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/value_operations/value_operations.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"

namespace duckdb {

struct ValueOperations {
	//===--------------------------------------------------------------------===//
	// Numeric Operations
	//===--------------------------------------------------------------------===//
	// A + B
	static void Add(const Value &left, const Value &right, Value &result);
	// A - B
	static void Subtract(const Value &left, const Value &right, Value &result);
	// A * B
	static void Multiply(const Value &left, const Value &right, Value &result);
	// A / B
	static void Divide(const Value &left, const Value &right, Value &result);
	// MIN(A, B)
	static void Min(const Value &left, const Value &right, Value &result);
	// MAX(A, B)
	static void Max(const Value &left, const Value &right, Value &result);
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
};
} // namespace duckdb
