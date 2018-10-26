//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/vector_operations.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>

#include "common/types/vector.hpp"

namespace duckdb {

// VectorOperations contains a set of operations that operate on sets of
// vectors. In general, the operators must all have the same type, otherwise an
// exception is thrown. Note that the functions underneath use restrict
// pointers, hence the data that the vectors point to (and hence the vector
// themselves) should not be equal! For example, if you call the function Add(A,
// B, A) then ASSERT_RESTRICT will be triggered. Instead call AddInPlace(A, B)
// or Add(A, B, C)
struct VectorOperations {
	//===--------------------------------------------------------------------===//
	// Numeric Binary Operators
	//===--------------------------------------------------------------------===//
	//! result = A + B
	static void Add(Vector &A, Vector &B, Vector &result);
	//! result = A - B
	static void Subtract(Vector &A, Vector &B, Vector &result);
	//! result = A * B
	static void Multiply(Vector &A, Vector &B, Vector &result);
	//! result = A / B
	static void Divide(Vector &A, Vector &B, Vector &result);
	//! result = A % B
	static void Modulo(Vector &A, Vector &B, Vector &result);

	//===--------------------------------------------------------------------===//
	// In-Place Operators
	//===--------------------------------------------------------------------===//
	//! A += B
	static void AddInPlace(Vector &A, Vector &B);
	//! A += B
	static void AddInPlace(Vector &A, int64_t B);

	//===--------------------------------------------------------------------===//
	// Numeric Functions
	//===--------------------------------------------------------------------===//
	//! result = ABS(A)
	static void Abs(Vector &A, Vector &result);

	//===--------------------------------------------------------------------===//
	// Bitwise Operators
	//===--------------------------------------------------------------------===//
	//! result = A ^ B
	static void BitwiseXOR(Vector &A, Vector &B, Vector &result);
	//! result = A & B
	static void BitwiseAND(Vector &A, Vector &B, Vector &result);
	//! result = A | B
	static void BitwiseOR(Vector &A, Vector &B, Vector &result);
	//! result = A << B
	static void BitwiseShiftLeft(Vector &A, Vector &B, Vector &result);
	//! result = A >> B
	static void BitwiseShiftRight(Vector &A, Vector &B, Vector &result);

	//===--------------------------------------------------------------------===//
	// In-Place Bitwise Operators
	//===--------------------------------------------------------------------===//
	//! A ^= B
	static void BitwiseXORInPlace(Vector &A, Vector &B);

	//===--------------------------------------------------------------------===//
	// NULL Operators
	//===--------------------------------------------------------------------===//
	//! result = IS NOT NULL(A)
	static void IsNotNull(Vector &A, Vector &result);
	//! result = IS NULL (A)
	static void IsNull(Vector &A, Vector &result);

	//===--------------------------------------------------------------------===//
	// Boolean Operations
	//===--------------------------------------------------------------------===//
	// result = A && B
	static void And(Vector &A, Vector &B, Vector &result);
	// result = A || B
	static void Or(Vector &A, Vector &B, Vector &result);
	// result = NOT(A)
	static void Not(Vector &A, Vector &result);

	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// result = A == B
	static void Equals(Vector &A, Vector &B, Vector &result);
	// result = A != B
	static void NotEquals(Vector &A, Vector &B, Vector &result);
	// result = A > B
	static void GreaterThan(Vector &A, Vector &B, Vector &result);
	// result = A >= B
	static void GreaterThanEquals(Vector &A, Vector &B, Vector &result);
	// result = A < B
	static void LessThan(Vector &A, Vector &B, Vector &result);
	// result = A <= B
	static void LessThanEquals(Vector &A, Vector &B, Vector &result);

	//===--------------------------------------------------------------------===//
	// Aggregates
	//===--------------------------------------------------------------------===//
	// SUM(A)
	static Value Sum(Vector &A);
	// COUNT(A)
	static Value Count(Vector &A);
	// MAX(A)
	static Value Max(Vector &A);
	// MIN(A)
	static Value Min(Vector &A);
	// Returns whether or not a vector has a NULL value
	static bool HasNull(Vector &A);
	// Maximum string length of the vector, only works on string vectors!
	static Value MaximumStringLength(Vector &A);
	// Check if any value is true in a bool vector
	static Value AnyTrue(Vector &A);
	// Check if all values are true in a bool vector
	static Value AllTrue(Vector &A);

	//! CASE expressions, ternary op
	//! result = check ? A : B
	static void Case(Vector &check, Vector &A, Vector &B, Vector &result);

	// Returns true if the vector contains an instance of Value
	static bool Contains(Vector &vector, Value &value);

	//===--------------------------------------------------------------------===//
	// Scatter methods
	//===--------------------------------------------------------------------===//
	struct Scatter {
		// dest[i] = source.data[i]
		static void Set(Vector &source, Vector &dest);
		// dest[i] += source.data[i]
		static void Add(Vector &source, Vector &dest);
		// dest[i] = max(dest[i], source.data[i])
		static void Max(Vector &source, Vector &dest);
		// dest[i] = min(dest[i], source.data[i])
		static void Min(Vector &source, Vector &dest);
		// dest[i] = dest[i] + 1
		static void AddOne(Vector &source, Vector &dest);
		//! dest[i] = dest[i]
		static void SetFirst(Vector &source, Vector &dest);
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
	// result = HASH(A)
	static void Hash(Vector &A, Vector &result);
	// A ^= HASH(B)
	static void CombineHash(Vector &hashes, Vector &B);

	//===--------------------------------------------------------------------===//
	// Generate functions
	//===--------------------------------------------------------------------===//
	static void GenerateSequence(Vector &source, int64_t start = 0,
	                             int64_t increment = 1);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	// Copy the data from source to target, casting if the types don't match
	static void Cast(Vector &source, Vector &result);
	// Copy the data of <source> to the target location
	static void Copy(Vector &source, void *target, size_t offset = 0,
	                 size_t element_count = 0);
	// Copy the data of <source> to the target vector
	static void Copy(Vector &source, Vector &target, size_t offset = 0);
	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>
	static void CopyNull(Vector &source, void *target, size_t offset = 0,
	                     size_t element_count = 0);
	// Appends the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source
	static void AppendNull(Vector &source, Vector &target);

	// Set all elements of the vector to the given constant value
	static void Set(Vector &result, Value value);
	//! For every value in result, set result[i] = left[sel_vector[i]]
	static void ApplySelectionVector(Vector &left, Vector &result,
	                                 sel_t *sel_vector);
	//===--------------------------------------------------------------------===//
	// Exec
	//===--------------------------------------------------------------------===//
	template <class T>
	static void Exec(sel_t *sel_vector, size_t count, T &&fun,
	                 size_t offset = 0) {
		size_t i = offset;
		if (sel_vector) {
			//#pragma GCC ivdep
			for (; i < count; i++) {
				fun(sel_vector[i], i);
			}
		} else {
			//#pragma GCC ivdep
			for (; i < count; i++) {
				fun(i, i);
			}
		}
	}
	//! Exec over the set of indexes, calls the callback function with (i) =
	//! index, dependent on selection vector and (k) = count
	template <class T>
	static void Exec(Vector &vector, T &&fun, size_t offset = 0,
	                 size_t count = 0) {
		if (count == 0) {
			count = vector.count;
		} else {
			count += offset;
		}
		Exec(vector.sel_vector, count, fun, offset);
	}

	//! Exec over a specific type. Note that it is up to the caller to verify
	//! that the vector passed in has the correct type for the iteration! This
	//! is equivalent to calling ::Exec() and performing data[i] for
	//! every entry
	template <typename T, class FUNC>
	static void ExecType(Vector &vector, FUNC &&fun, size_t offset = 0,
	                     size_t limit = 0) {
		auto data = (T *)vector.data;
		VectorOperations::Exec(vector,
		                       [&](size_t i, size_t k) { fun(data[i], i, k); },
		                       offset, limit);
	}
};
} // namespace duckdb
