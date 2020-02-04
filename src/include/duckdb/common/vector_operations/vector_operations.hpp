//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/vector_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"

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
	// Returns whether or not a vector has a NULL value
	static bool HasNull(Vector &A);

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
	// Select Comparison Operations
	//===--------------------------------------------------------------------===//
	// result = A == B
	static index_t SelectEquals(Vector &A, Vector &B, sel_t result[]);
	// result = A != B
	static index_t SelectNotEquals(Vector &A, Vector &B, sel_t result[]);
	// result = A > B
	static index_t SelectGreaterThan(Vector &A, Vector &B, sel_t result[]);
	// result = A >= B
	static index_t SelectGreaterThanEquals(Vector &A, Vector &B, sel_t result[]);
	// result = A < B
	static index_t SelectLessThan(Vector &A, Vector &B, sel_t result[]);
	// result = A <= B
	static index_t SelectLessThanEquals(Vector &A, Vector &B, sel_t result[]);

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
		//! Instead, this can **only** be used when the destination type is TypeId::INT64
		static void AddOne(Vector &source, Vector &dest);
		//! dest[i] = dest[i]
		static void SetFirst(Vector &source, Vector &dest);
		// dest[i] = dest[i] + source
		static void Add(int64_t source, void **dest, index_t length);
		//! Similar to Set, but also write NullValue<T> if set_null = true, or ignore null values entirely if set_null =
		//! false
		static void SetAll(Vector &source, Vector &dest, bool set_null = false, index_t offset = 0);
	};
	// make sure dest.count is set for gather methods!
	struct Gather {
		//! dest.data[i] = ptr[i]. If set_null is true, NullValue<T> is checked for and converted to the nullmask in
		//! dest. If set_null is false, NullValue<T> is ignored.
		static void Set(Vector &source, Vector &dest, bool set_null = true, index_t offset = 0);
		//! Append the values from source to the dest vector. If set_null is true, NullValue<T> is checked for and
		//! converted to the nullmask in dest. If set_null is false, NullValue<T> is ignored. If offset is set, it is
		//! added to
		static void Append(Vector &source, Vector &dest, index_t offset = 0, bool set_null = true);
	};

	//===--------------------------------------------------------------------===//
	// Sort functions
	//===--------------------------------------------------------------------===//
	// Sort the vector, setting the given selection vector to a sorted state.
	static void Sort(Vector &vector, sel_t result[]);
	// Sort the vector, setting the given selection vector to a sorted state
	// while ignoring NULL values.
	static void Sort(Vector &vector, sel_t *result_vector, index_t count, sel_t result[]);
	// Checks whether or not the vector contains only unique values
	static bool Unique(Vector &vector);
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
	static void GenerateSequence(Vector &result, int64_t start = 0, int64_t increment = 1,
	                             bool ignore_sel_vector = false);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	// Cast the data from the source type to the target type
	static void Cast(Vector &source, Vector &result, SQLType source_type, SQLType target_type);
	// Cast the data from the source type to the target type
	static void Cast(Vector &source, Vector &result);
	// Copy the data of <source> to the target location
	static void Copy(Vector &source, void *target, index_t offset = 0, index_t element_count = 0);
	// Copy the data of <source> to the target vector
	static void Copy(Vector &source, Vector &target, index_t offset = 0);
	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>. Used to store data without separate NULL mask.
	static void CopyToStorage(Vector &source, void *target, index_t offset = 0, index_t element_count = 0);
	// Appends the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source. Used to go back from storage to a
	// nullmask.
	static void AppendFromStorage(Vector &source, Vector &target, bool has_null = true);

	// Set all elements of the vector to the given constant value
	static void Set(Vector &result, Value value);
	//! Fill the null mask of a value, setting every NULL value to NullValue<T>()
	static void FillNullMask(Vector &v);
	//===--------------------------------------------------------------------===//
	// Exec
	//===--------------------------------------------------------------------===//
	template <class T> static void Exec(sel_t *sel_vector, index_t count, T &&fun, index_t offset = 0) {
		index_t i = offset;
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
	template <class T> static void Exec(const Vector &vector, T &&fun, index_t offset = 0, index_t count = 0) {
		if (vector.vector_type == VectorType::CONSTANT_VECTOR) {
			assert(count == 0 && offset == 0);
			Exec(nullptr, 1, fun, offset);
		} else {
			assert(vector.vector_type == VectorType::FLAT_VECTOR);
			if (count == 0) {
				count = vector.count;
			} else {
				count += offset;
			}
			Exec(vector.sel_vector, count, fun, offset);
		}
	}

	//! Exec over a specific type. Note that it is up to the caller to verify
	//! that the vector passed in has the correct type for the iteration! This
	//! is equivalent to calling ::Exec() and performing data[i] for
	//! every entry
	template <typename T, class FUNC>
	static void ExecType(Vector &vector, FUNC &&fun, index_t offset = 0, index_t limit = 0) {
		auto data = (T *)vector.GetData();
		VectorOperations::Exec(
		    vector, [&](index_t i, index_t k) { fun(data[i], i, k); }, offset, limit);
	}

	template <typename T, class FUNC> static void ExecNumeric(Vector &vector, FUNC &&fun) {
		if (vector.vector_type == VectorType::SEQUENCE_VECTOR) {
			int64_t start, increment;
			vector.GetSequence(start, increment);
			for (index_t i = 0; i < vector.count; i++) {
				index_t idx = vector.sel_vector ? vector.sel_vector[i] : i;
				fun((T)(start + increment * idx), idx, i);
			}
		} else if (vector.vector_type == VectorType::CONSTANT_VECTOR) {
			auto data = (T *)vector.GetData();
			fun(data[0], 0, 0);
		} else {
			assert(vector.vector_type == VectorType::FLAT_VECTOR);
			auto data = (T *)vector.GetData();
			VectorOperations::Exec(vector, [&](index_t i, index_t k) { fun(data[i], i, k); });
		}
	}
};
} // namespace duckdb
