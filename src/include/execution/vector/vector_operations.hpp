
#pragma once

#include "common/types/vector.hpp"

namespace duckdb {

class VectorOperations {
  public:
	//===--------------------------------------------------------------------===//
	// Numeric Operations
	//===--------------------------------------------------------------------===//
	// A + B
	static void Add(Vector &left, Vector &right, Vector &result);
	// A - B
	static void Subtract(Vector &left, Vector &right, Vector &result);
	// A * B
	static void Multiply(Vector &left, Vector &right, Vector &result);
	// A / B
	static void Divide(Vector &left, Vector &right, Vector &result);

	//===--------------------------------------------------------------------===//
	// Boolean Operations
	//===--------------------------------------------------------------------===//
	// A && B
	static void And(Vector &left, Vector &right, Vector &result);
	// A || B
	static void Or(Vector &left, Vector &right, Vector &result);

	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// A == B
	static void Equals(Vector &left, Vector &right, Vector &result);
	// A != B
	static void NotEquals(Vector &left, Vector &right, Vector &result);
	// A > B
	static void GreaterThan(Vector &left, Vector &right, Vector &result);
	// A >= B
	static void GreaterThanEquals(Vector &left, Vector &right, Vector &result);
	// A < B
	static void LessThan(Vector &left, Vector &right, Vector &result);
	// A <= B
	static void LessThanEquals(Vector &left, Vector &right, Vector &result);

	//===--------------------------------------------------------------------===//
	// Aggregates
	//===--------------------------------------------------------------------===//
	// SUM(A)
	static void Sum(Vector &source, Vector &result);
	// COUNT(A)
	static void Count(Vector &source, Vector &result);
	// AVG(A)
	static void Average(Vector &source, Vector &result);

	//===--------------------------------------------------------------------===//
	// Hash functions
	//===--------------------------------------------------------------------===//
	// HASH(A)
	static void Hash(Vector &source, Vector &result);
	// COMBINE(A, HASH(B))
	static void CombineHash(Vector &left, Vector &right, Vector &result);

	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	static void Copy(Vector &source, void *target);
};
}
