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
	// Distinction Operations
	//===--------------------------------------------------------------------===//
	// A == B, NULLs equal
	static bool NotDistinctFrom(const Value &left, const Value &right);
	// A != B, NULLs equal
	static bool DistinctFrom(const Value &left, const Value &right);
	// A > B, NULLs last
	static bool DistinctGreaterThan(const Value &left, const Value &right);
	// A >= B, NULLs last
	static bool DistinctGreaterThanEquals(const Value &left, const Value &right);
	// A < B, NULLs last
	static bool DistinctLessThan(const Value &left, const Value &right);
	// A <= B, NULLs last
	static bool DistinctLessThanEquals(const Value &left, const Value &right);
};
} // namespace duckdb
