//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/vector/vector_operations.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/vector.hpp"

namespace duckdb {

// VectorOperations contains a set of operations that operate on sets of
// vectors. In general, the operators must all have the same type, otherwise an
// exception is thrown.
struct VectorOperations {
	typedef void (*vector_function)(Vector &left, Vector &right, Vector &result,
	                                bool can_have_null);

	//===--------------------------------------------------------------------===//
	// Numeric Operations
	//===--------------------------------------------------------------------===//
	// A + B
	static void Add(Vector &left, Vector &right, Vector &result,
	                bool can_have_null = true);
	// A - B
	static void Subtract(Vector &left, Vector &right, Vector &result,
	                     bool can_have_null = true);
	// A * B
	static void Multiply(Vector &left, Vector &right, Vector &result,
	                     bool can_have_null = true);
	// A / B
	static void Divide(Vector &left, Vector &right, Vector &result,
	                   bool can_have_null = true);
	// A % B
	static void Modulo(Vector &left, Vector &right, Vector &result,
	                   bool can_have_null = true);

	// A + B
	static void Add(Vector &left, int64_t right, Vector &result,
	                bool can_have_null = true);
	// A - B
	static void Subtract(Vector &left, int64_t right, Vector &result,
	                     bool can_have_null = true);
	// A * B
	static void Multiply(Vector &left, int64_t right, Vector &result,
	                     bool can_have_null = true);
	// A / B
	static void Divide(Vector &left, int64_t right, Vector &result,
	                   bool can_have_null = true);
	// A % B
	static void Modulo(Vector &left, int64_t right, Vector &result,
	                   bool can_have_null = true);

	// A + B
	static void Add(int64_t left, Vector &right, Vector &result,
	                bool can_have_null = true);
	// A - B
	static void Subtract(int64_t left, Vector &right, Vector &result,
	                     bool can_have_null = true);
	// A * B
	static void Multiply(int64_t left, Vector &right, Vector &result,
	                     bool can_have_null = true);
	// A / B
	static void Divide(int64_t left, Vector &right, Vector &result,
	                   bool can_have_null = true);
	// A % B
	static void Modulo(int64_t left, Vector &right, Vector &result,
	                   bool can_have_null = true);

	//===--------------------------------------------------------------------===//
	// Boolean Operations
	//===--------------------------------------------------------------------===//
	// A && B
	static void And(Vector &left, Vector &right, Vector &result,
	                bool can_have_null = true);
	// A || B
	static void Or(Vector &left, Vector &right, Vector &result,
	               bool can_have_null = true);

	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// A == B
	static void Equals(Vector &left, Vector &right, Vector &result,
	                   bool can_have_null = true);
	// A != B
	static void NotEquals(Vector &left, Vector &right, Vector &result,
	                      bool can_have_null = true);
	// A > B
	static void GreaterThan(Vector &left, Vector &right, Vector &result,
	                        bool can_have_null = true);
	// A >= B
	static void GreaterThanEquals(Vector &left, Vector &right, Vector &result,
	                              bool can_have_null = true);
	// A < B
	static void LessThan(Vector &left, Vector &right, Vector &result,
	                     bool can_have_null = true);
	// A <= B
	static void LessThanEquals(Vector &left, Vector &right, Vector &result,
	                           bool can_have_null = true);

	//===--------------------------------------------------------------------===//
	// Aggregates
	//===--------------------------------------------------------------------===//
	// SUM(A)
	static Value Sum(Vector &source, bool can_have_null = true);
	// COUNT(A)
	static Value Count(Vector &source, bool can_have_null = true);
	// AVG(A)
	static Value Average(Vector &source, bool can_have_null = true);
	// MAX(A)
	static Value Max(Vector &left, bool can_have_null = true);
	// MIN(A)
	static Value Min(Vector &left, bool can_have_null = true);
	// Returns whether or not a vector has a NULL value
	static bool HasNull(Vector &left, bool can_have_null = true);
	// Maximum string length of the vector, only works on string vectors!
	static Value MaximumStringLength(Vector &left, bool can_have_null = true);

	// CASE expressions, ternary op
	static void Case(Vector &check, Vector &res_true, Vector &res_false,
	                 Vector &result);

	//===--------------------------------------------------------------------===//
	// Scatter methods
	//===--------------------------------------------------------------------===//
	struct Scatter {
		// dest[i] = source.data[i]
		static void Set(Vector &source, Vector &dest);
		// dest[i] = dest[i] + source.data[i]
		static void Add(Vector &source, Vector &dest);
		// dest[i] = max(dest[i], source.data[i])
		static void Max(Vector &source, Vector &dest);
		// dest[i] = min(dest[i], source.data[i])
		static void Min(Vector &source, Vector &dest);
		// dest[i] = len(source.data[i])
		static void SetCount(Vector &source, Vector &dest);
		// dest[i] = dest[i] + 1
		static void AddOne(Vector &source, Vector &dest);

		// dest[i] = dest[i] + source
		static void Add(int64_t source, void **dest, size_t length);
	};
	// make sure dest.count is set for gather methods!
	struct Gather {
		// dest.data[i] = ptr[i]
		static void Set(Vector &source, Vector &dest);
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
	static void Cast(Vector &source, Vector &result);
	// Copy the data of <source> to the target location
	static void Copy(Vector &source, void *target, size_t element_count = 0,
	                 size_t offset = 0);
	// Copy the data of <source> to the target vector
	static void Copy(Vector &source, Vector &target, size_t offset = 0);
	// Set all elements of the vector to the given constant value
	static void Set(Vector &result, Value value);
};
} // namespace duckdb
