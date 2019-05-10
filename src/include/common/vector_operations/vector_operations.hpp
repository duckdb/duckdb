//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/vector_operations/vector_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/vector.hpp"

#include <functional>

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

	//! A %= B
	static void ModuloInPlace(Vector &A, Vector &B);
	//! A %= B
	static void ModuloInPlace(Vector &A, int64_t B);

	//===--------------------------------------------------------------------===//
	// Numeric Functions
	//===--------------------------------------------------------------------===//
	//! result = ABS(A)
	static void Abs(Vector &A, Vector &result);
	static void Round(Vector &A, Vector &B, Vector &result);

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
	// String Operations
	//===--------------------------------------------------------------------===//
	// result = A LIKE B
	static void Like(Vector &A, Vector &B, Vector &result);
	// result = A NOT LIKE B
	static void NotLike(Vector &A, Vector &B, Vector &result);

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
	static bool AnyTrue(Vector &A);
	// Check if all values are true in a bool vector
	static bool AllTrue(Vector &A);

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
		//! dest[i] = dest[i] + 1
		//! For this operation the destination type does not need to match source.type
		//! Instead, this can **only** be used when the destination type is TypeId::BIGINT
		static void AddOne(Vector &source, Vector &dest);
		//! dest[i] = dest[i]
		static void SetFirst(Vector &source, Vector &dest);
		// dest[i] = dest[i] + source
		static void Add(int64_t source, void **dest, uint64_t length);
	};
	// make sure dest.count is set for gather methods!
	struct Gather {
		//! dest.data[i] = ptr[i]. If set_null is true, NullValue<T> is checked for and converted to the nullmask in
		//! dest. If set_null is false, NullValue<T> is ignored.
		static void Set(Vector &source, Vector &dest, bool set_null = true);
	};

	//===--------------------------------------------------------------------===//
	// Sort functions
	//===--------------------------------------------------------------------===//
	// Sort the vector, setting the given selection vector to a sorted state.
	static void Sort(Vector &vector, sel_t result[]);
	// Sort the vector, setting the given selection vector to a sorted state
	// while ignoring NULL values.
	static void Sort(Vector &vector, sel_t *result_vector, uint64_t count, sel_t result[]);
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
	static void GenerateSequence(Vector &result, int64_t start = 0, int64_t increment = 1);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	// Cast the data from the source type to the target type
	static void Cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type);
	// Cast the data from the source type to the target type
	static void Cast(Vector &source, Vector &result);
	// Copy the data of <source> to the target location
	static void Copy(Vector &source, void *target, uint64_t offset = 0, uint64_t element_count = 0);
	// Copy the data of <source> to the target vector
	static void Copy(Vector &source, Vector &target, uint64_t offset = 0);
	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>. Used to store data without separate NULL mask.
	static void CopyToStorage(Vector &source, void *target, uint64_t offset = 0, uint64_t element_count = 0);
	// Appends the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source. Used to go back from storage to a
	// nullmask.
	static void AppendFromStorage(Vector &source, Vector &target);

	// Set all elements of the vector to the given constant value
	static void Set(Vector &result, Value value);
	//===--------------------------------------------------------------------===//
	// Exec
	//===--------------------------------------------------------------------===//
	template <class T> static void Exec(sel_t *sel_vector, uint64_t count, T &&fun, uint64_t offset = 0) {
		uint64_t i = offset;
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
	template <class T> static void Exec(const Vector &vector, T &&fun, uint64_t offset = 0, uint64_t count = 0) {
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
	static void ExecType(Vector &vector, FUNC &&fun, uint64_t offset = 0, uint64_t limit = 0) {
		auto data = (T *)vector.data;
		VectorOperations::Exec(
		    vector, [&](size_t i, size_t k) { fun(data[i], i, k); }, offset, limit);
	}
	template <class FUNC> static void BinaryExec(Vector &a, Vector &b, Vector &result, FUNC &&fun) {
		// it might be the case that not everything has a selection vector
		// as constants do not need a selection vector
		// check if we are using a selection vector
		if (!a.IsConstant()) {
			result.sel_vector = a.sel_vector;
			result.count = a.count;
		} else if (!b.IsConstant()) {
			result.sel_vector = b.sel_vector;
			result.count = b.count;
		} else {
			result.sel_vector = nullptr;
			result.count = 1;
		}

		// now check for constants
		// we handle constants by multiplying the index access by 0 to avoid 2^3
		// branches in the code
		uint64_t a_mul = a.IsConstant() ? 0 : 1;
		uint64_t b_mul = b.IsConstant() ? 0 : 1;

		assert(a.IsConstant() || a.count == result.count);
		assert(b.IsConstant() || b.count == result.count);

		VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) { fun(a_mul * i, b_mul * i, i); });
	}
	template <class FUNC> static void TernaryExec(Vector &a, Vector &b, Vector &c, Vector &result, FUNC &&fun) {
		// it might be the case that not everything has a selection vector
		// as constants do not need a selection vector
		// check if we are using a selection vector
		if (!a.IsConstant()) {
			result.sel_vector = a.sel_vector;
			result.count = a.count;
		} else if (!b.IsConstant()) {
			result.sel_vector = b.sel_vector;
			result.count = b.count;
		} else if (!c.IsConstant()) {
			result.sel_vector = c.sel_vector;
			result.count = c.count;
		} else {
			result.sel_vector = nullptr;
			result.count = 1;
		}

		// now check for constants
		// we handle constants by multiplying the index access by 0 to avoid 2^3
		// branches in the code
		uint64_t a_mul = a.IsConstant() ? 0 : 1;
		uint64_t b_mul = b.IsConstant() ? 0 : 1;
		uint64_t c_mul = c.IsConstant() ? 0 : 1;

		assert(a.IsConstant() || a.count == result.count);
		assert(b.IsConstant() || b.count == result.count);
		assert(c.IsConstant() || c.count == result.count);

		VectorOperations::Exec(result, [&](uint64_t i, uint64_t k) { fun(a_mul * i, b_mul * i, c_mul * i, i); });
	}
};
} // namespace duckdb
