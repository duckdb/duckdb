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

namespace duckdb {
class CastFunctionSet;
struct GetCastFunctionInput;

// VectorOperations contains a set of operations that operate on sets of
// vectors. In general, the operators must all have the same type, otherwise an
// exception is thrown. Note that the functions underneath use restrict
// pointers, hence the data that the vectors point to (and hence the vector
// themselves) should not be equal! For example, if you call the function Add(A,
// B, A) then ASSERT_RESTRICT will be triggered. Instead call AddInPlace(A, B)
// or Add(A, B, C)
struct VectorOperations {
	//===--------------------------------------------------------------------===//
	// In-Place Operators
	//===--------------------------------------------------------------------===//
	//! left += delta
	static void AddInPlace(Vector &left, int64_t delta);

	//===--------------------------------------------------------------------===//
	// NULL Operators
	//===--------------------------------------------------------------------===//
	//! result = IS NOT NULL(input)
	static void IsNotNull(const Vector &arg, Vector &result);
	//! result = IS NULL (input)
	static void IsNull(const Vector &input, Vector &result);
	// Returns whether or not arg vector has a NULL value
	static bool HasNull(const Vector &input);
	static bool HasNotNull(const Vector &input);
	//! Count the number of not-NULL values.
	static idx_t CountNotNull(const Vector &input);

	//===--------------------------------------------------------------------===//
	// Boolean Operations
	//===--------------------------------------------------------------------===//
	// result = left && right
	static void And(const Vector &left, const Vector &right, Vector &result);
	// result = left || right
	static void Or(const Vector &left, const Vector &right, Vector &result);
	// result = NOT(input)
	static void Not(const Vector &input, Vector &result);

	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// result = left == right
	static void Equals(const Vector &left, const Vector &right, Vector &result);
	// result = left != right
	static void NotEquals(const Vector &left, const Vector &right, Vector &result);
	// result = left > right
	static void GreaterThan(const Vector &left, const Vector &right, Vector &result);
	// result = left >= right
	static void GreaterThanEquals(const Vector &left, const Vector &right, Vector &result);
	// result = left < right
	static void LessThan(const Vector &left, const Vector &right, Vector &result);
	// result = left <= right
	static void LessThanEquals(const Vector &left, const Vector &right, Vector &result);

	// result = -1 if left < right, 0 if left == right, 1 if left > right (stored in int8_t TINYINT result vector)
	static void Comparator(const Vector &left, const Vector &right, Vector &result);
	// result = -1 if left < right, 0 if left == right, 1 if left > right; fills exactly count rows (for select paths)
	static void ComparatorFill(const Vector &left, const Vector &right, Vector &result, idx_t count);

	// result = A != B with nulls being equal
	static void DistinctFrom(const Vector &left, const Vector &right, Vector &result);
	// result := A == B with nulls being equal
	static void NotDistinctFrom(const Vector &left, const Vector &right, Vector &result);
	// result := comparator(A, B) with nulls being maximal (NULLS LAST), returns -1/0/1 as int8_t
	static void DistinctComparator(const Vector &left, const Vector &right, Vector &result);
	// result := comparator(A, B) with nulls being minimal (NULLS FIRST), returns -1/0/1 as int8_t
	static void DistinctComparatorNullsFirst(const Vector &left, const Vector &right, Vector &result);
	// DistinctComparator/NullsFirst variants that fill exactly count rows (for select paths with SelectionVector)
	static void DistinctComparatorFill(const Vector &left, const Vector &right, Vector &result, idx_t count);
	static void DistinctComparatorNullsFirstFill(const Vector &left, const Vector &right, Vector &result, idx_t count);

	//===--------------------------------------------------------------------===//
	// Select Comparisons
	//===--------------------------------------------------------------------===//
	static idx_t Equals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
	                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                    optional_ptr<ValidityMask> null_mask = nullptr);
	static idx_t NotEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                       idx_t count, optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                       optional_ptr<ValidityMask> null_mask = nullptr);
	static idx_t GreaterThan(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                         idx_t count, optional_ptr<SelectionVector> true_sel,
	                         optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask = nullptr);
	static idx_t GreaterThanEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                               idx_t count, optional_ptr<SelectionVector> true_sel,
	                               optional_ptr<SelectionVector> false_sel,
	                               optional_ptr<ValidityMask> null_mask = nullptr);
	static idx_t LessThan(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
	                      optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                      optional_ptr<ValidityMask> null_mask = nullptr);
	static idx_t LessThanEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                            idx_t count, optional_ptr<SelectionVector> true_sel,
	                            optional_ptr<SelectionVector> false_sel,
	                            optional_ptr<ValidityMask> null_mask = nullptr);

	// true := A != B with nulls being equal
	static idx_t DistinctFrom(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                          idx_t count, optional_ptr<SelectionVector> true_sel,
	                          optional_ptr<SelectionVector> false_sel);
	// true := A == B with nulls being equal
	static idx_t NotDistinctFrom(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                             idx_t count, optional_ptr<SelectionVector> true_sel,
	                             optional_ptr<SelectionVector> false_sel);
	// true := A > B with nulls being maximal
	static idx_t DistinctGreaterThan(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                                 idx_t count, optional_ptr<SelectionVector> true_sel,
	                                 optional_ptr<SelectionVector> false_sel,
	                                 optional_ptr<ValidityMask> null_mask = nullptr);
	// true := A >= B with nulls being maximal
	static idx_t DistinctGreaterThanEquals(const Vector &left, const Vector &right,
	                                       optional_ptr<const SelectionVector> sel, idx_t count,
	                                       optional_ptr<SelectionVector> true_sel,
	                                       optional_ptr<SelectionVector> false_sel,
	                                       optional_ptr<ValidityMask> null_mask = nullptr);
	// true := A < B with nulls being maximal
	static idx_t DistinctLessThan(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                              idx_t count, optional_ptr<SelectionVector> true_sel,
	                              optional_ptr<SelectionVector> false_sel,
	                              optional_ptr<ValidityMask> null_mask = nullptr);
	// true := A <= B with nulls being maximal
	static idx_t DistinctLessThanEquals(const Vector &left, const Vector &right,
	                                    optional_ptr<const SelectionVector> sel, idx_t count,
	                                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                                    optional_ptr<ValidityMask> null_mask = nullptr);
	// true := A > B with nulls being minimal
	static idx_t DistinctGreaterThanNullsFirst(const Vector &left, const Vector &right,
	                                           optional_ptr<const SelectionVector> sel, idx_t count,
	                                           optional_ptr<SelectionVector> true_sel,
	                                           optional_ptr<SelectionVector> false_sel,
	                                           optional_ptr<ValidityMask> null_mask = nullptr);
	// true := A < B with nulls being minimal
	static idx_t DistinctLessThanNullsFirst(const Vector &left, const Vector &right,
	                                        optional_ptr<const SelectionVector> sel, idx_t count,
	                                        optional_ptr<SelectionVector> true_sel,
	                                        optional_ptr<SelectionVector> false_sel,
	                                        optional_ptr<ValidityMask> null_mask = nullptr);

	//===--------------------------------------------------------------------===//
	// Nested Comparisons
	//===--------------------------------------------------------------------===//
	// true := A != B with nulls being equal
	static idx_t NestedNotEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                             idx_t count, optional_ptr<SelectionVector> true_sel,
	                             optional_ptr<SelectionVector> false_sel,
	                             optional_ptr<ValidityMask> null_mask = nullptr);
	// true := A == B with nulls being equal
	static idx_t NestedEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
	                          idx_t count, optional_ptr<SelectionVector> true_sel,
	                          optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask = nullptr);

	//===--------------------------------------------------------------------===//
	// Hash functions
	//===--------------------------------------------------------------------===//
	// hashes = HASH(input)
	static void Hash(const Vector &input, Vector &hashes, idx_t count);
	static void Hash(const Vector &input, Vector &hashes, const SelectionVector &rsel, idx_t count);
	//! Convenience overload without explicit count - count is derived from input.size()
	static void Hash(const Vector &input, Vector &hashes);
	// hashes ^= HASH(input)
	static void CombineHash(Vector &hashes, const Vector &input, idx_t count);
	static void CombineHash(Vector &hashes, const Vector &input, const SelectionVector &rsel, idx_t count);
	//! Convenience overload without explicit count - count is derived from the input vectors (with mismatch check)
	static void CombineHash(Vector &hashes, const Vector &input);

	//===--------------------------------------------------------------------===//
	// Generate functions
	//===--------------------------------------------------------------------===//
	static void GenerateSequence(Vector &result, idx_t count, int64_t start = 0, int64_t increment = 1);
	static void GenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start = 0,
	                             int64_t increment = 1);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	//! Cast the data from the source type to the target type. Any elements that could not be converted are turned into
	//! NULLs. If any elements cannot be converted, returns false and fills in the error_message. If no error message is
	//! provided, an exception is thrown instead.
	DUCKDB_API static bool TryCast(CastFunctionSet &set, GetCastFunctionInput &input, Vector &source, Vector &result,
	                               idx_t count, string *error_message, bool strict = false,
	                               const bool nullify_parent = false);
	DUCKDB_API static bool DefaultTryCast(Vector &source, Vector &result, idx_t count, string *error_message,
	                                      bool strict = false);
	DUCKDB_API static bool TryCast(ClientContext &context, Vector &source, Vector &result, idx_t count,
	                               string *error_message, bool strict = false, const bool nullify_parent = false);
	//! Cast the data from the source type to the target type. Throws an exception if the cast fails.
	DUCKDB_API static void Cast(ClientContext &context, Vector &source, Vector &result, idx_t count,
	                            bool strict = false);
	DUCKDB_API static void DefaultCast(Vector &source, Vector &result, idx_t count, bool strict = false);

	// Copy the data of <source> to the target vector
	static void Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
	                 idx_t target_offset);
	static void Copy(const Vector &source, Vector &target, const SelectionVector &sel, idx_t source_count,
	                 idx_t source_offset, idx_t target_offset);
	static void Copy(const Vector &source, Vector &target, const SelectionVector &sel, idx_t source_count,
	                 idx_t source_offset, idx_t target_offset, idx_t copy_count);

	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>. Used to store data without separate NULL mask.
	static void WriteToStorage(const Vector &source, data_ptr_t target);
	// Reads the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source. Used to go back from storage to a proper vector
	static void ReadFromStorage(data_ptr_t source, idx_t count, Vector &result);
};
} // namespace duckdb
