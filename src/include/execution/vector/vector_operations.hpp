
#pragma once

#include "common/types/vector.hpp"

namespace duckdb {

struct VectorOperations {
  	typedef void (*vector_function)(Vector &left, Vector &right, Vector &result);

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
	// A % B
	static void Modulo(Vector &left, Vector &right, Vector &result);

	// A + B
	static void Add(Vector &left, int64_t right, Vector &result);
	// A - B
	static void Subtract(Vector &left, int64_t right, Vector &result);
	// A * B
	static void Multiply(Vector &left, int64_t right, Vector &result);
	// A / B
	static void Divide(Vector &left, int64_t right, Vector &result);
	// A % B
	static void Modulo(Vector &left, int64_t right, Vector &result);

	// A + B
	static void Add(int64_t left, Vector& right, Vector &result);
	// A - B
	static void Subtract(int64_t left, Vector& right, Vector &result);
	// A * B
	static void Multiply(int64_t left, Vector& right, Vector &result);
	// A / B
	static void Divide(int64_t left, Vector& right, Vector &result);
	// A % B
	static void Modulo(int64_t left, Vector& right, Vector &result);

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
	// MAX(A)
	static void Max(Vector &left, Vector &result);
	// MIN(A)
	static void Min(Vector &left, Vector &result);

	//===--------------------------------------------------------------------===//
	// Scatter methods
	//===--------------------------------------------------------------------===//
	struct Scatter {
		// if count == (size_t) -1, then source.count is used
		// otherwise only the first element of source is scattered [count] times

		// dest[i] = source.data[i]
		static void Set(Vector &source, void **dest, size_t count = (size_t) -1);
		// dest[i] = dest[i] + source.data[i]
		static void Add(Vector &source, void **dest, size_t count = (size_t) -1);
		// dest[i] = max(dest[i], source.data[i])
		static void Max(Vector &source, void **dest, size_t count = (size_t) -1);
		// dest[i] = min(dest[i], source.data[i])
		static void Min(Vector &source, void **dest, size_t count = (size_t) -1);

		// dest[i] = dest[i] + source
		static void Add(int64_t source, void **dest, size_t length);
	};
	// make sure dest.count is set for gather methods!
	struct Gather {
		// dest.data[i] = ptr[i]
		static void Set(void **source, Vector &dest);
	};
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
	// Copy the data from source to target, casting if the types don't match
	static void Cast(Vector& source, Vector& result);
	// Copy the data of <source> to the target location
	static void Copy(Vector &source, void *target);
};
}
