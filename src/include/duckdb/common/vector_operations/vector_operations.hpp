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
	// NULL Operators
	//===--------------------------------------------------------------------===//
	//! result = IS NOT NULL(A)
	static void IsNotNull(Vector &A, Vector &result);
	//! result = IS NULL (A)
	static void IsNull(Vector &A, Vector &result);
	// Returns whether or not a vector has a NULL value
	static bool HasNull(Vector &A);
	//! Creates a selection vector that points only to non-null values for the
	//! given null mask. Returns the amount of not-null values.
	//! result_assignment will be set to either result_vector (if there are null
	//! values) or to nullptr (if there are no null values)
	static idx_t NotNullSelVector(Vector &vector, sel_t *not_null_vector, sel_t *&result_assignment,
	                              sel_t *null_vector = nullptr);

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
	static idx_t SelectEquals(Vector &A, Vector &B, sel_t result[]);
	// result = A != B
	static idx_t SelectNotEquals(Vector &A, Vector &B, sel_t result[]);
	// result = A > B
	static idx_t SelectGreaterThan(Vector &A, Vector &B, sel_t result[]);
	// result = A >= B
	static idx_t SelectGreaterThanEquals(Vector &A, Vector &B, sel_t result[]);
	// result = A < B
	static idx_t SelectLessThan(Vector &A, Vector &B, sel_t result[]);
	// result = A <= B
	static idx_t SelectLessThanEquals(Vector &A, Vector &B, sel_t result[]);

	//===--------------------------------------------------------------------===//
	// Scatter methods
	//===--------------------------------------------------------------------===//
	struct Scatter {
		//! Similar to Set, but also write NullValue<T> if set_null = true, or ignore null values entirely if set_null =
		//! false
		static void SetAll(Vector &source, Vector &dest, bool set_null = false, idx_t offset = 0);
	};
	// make sure dest.count is set for gather methods!
	struct Gather {
		//! dest.data[i] = ptr[i]. If set_null is true, NullValue<T> is checked for and converted to the nullmask in
		//! dest. If set_null is false, NullValue<T> is ignored.
		static void Set(Vector &source, Vector &dest, bool set_null = true, idx_t offset = 0);
	};

	//===--------------------------------------------------------------------===//
	// Sort functions
	//===--------------------------------------------------------------------===//
	// Sort the vector, setting the given selection vector to a sorted state.
	static void Sort(Vector &vector, sel_t result[]);
	// Sort the vector, setting the given selection vector to a sorted state
	// while ignoring NULL values.
	static void Sort(Vector &vector, sel_t *result_vector, idx_t count, sel_t result[]);
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
	static void Copy(Vector &source, void *target, idx_t offset = 0, idx_t element_count = 0);
	// Copy the data of <source> to the target vector
	static void Copy(Vector &source, Vector &target, idx_t offset = 0);
	// Append the data of <source> to the target vector
	static void Append(Vector &source, Vector &target);
	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>. Used to store data without separate NULL mask.
	static void CopyToStorage(Vector &source, void *target, idx_t offset = 0, idx_t element_count = 0);
	// Reads the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source. Used to go back from storage to a proper vector
	static void ReadFromStorage(Vector &source, Vector &target);

	// Set all elements of the vector to the given constant value
	static void Set(Vector &result, Value value);
	//! Fill the null mask of a value, setting every NULL value to NullValue<T>()
	static void FillNullMask(Vector &v);
	//===--------------------------------------------------------------------===//
	// Exec
	//===--------------------------------------------------------------------===//
	template <typename T, class FUNC> static void ExecNumeric(Vector &vector, FUNC &&fun, SelectionVector &sel) {
		switch(vector.vector_type) {
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			vector.GetSequence(start, increment);
			for (idx_t i = 0; i < vector.size(); i++) {
				fun((T)(start + increment * sel.get_index(i)));
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto data = (T *)vector.GetData();
			for(idx_t i = 0; i < vector.size(); i++) {
				fun(data[sel.get_index(i)]);
			}
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for ExecNumeric");
		}
	}

	template <typename T, class FUNC> static void ExecNumeric(Vector &vector, FUNC &&fun) {
		switch(vector.vector_type) {
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			vector.GetSequence(start, increment);
			for (idx_t i = 0; i < vector.size(); i++) {
				fun((T)(start + increment * i));
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			auto data = (T *)vector.GetData();
			fun(data[0]);
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto data = (T *)vector.GetData();
			for(idx_t i = 0; i < vector.size(); i++) {
				fun(data[i]);
			}
			break;
		}
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel = vector.GetSelVector();
			auto &child = vector.GetDictionaryChild();
			ExecNumeric<T, FUNC>(child, fun, sel);
			break;
		}
		default:
			throw NotImplementedException("Unimplemented type for ExecNumeric");
		}
	}
};
} // namespace duckdb
