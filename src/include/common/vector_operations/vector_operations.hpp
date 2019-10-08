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
	static void Ceil(Vector &A, Vector &result);
	static void Floor(Vector &A, Vector &result);
	static void CbRt(Vector &A, Vector &result);
	static void Degrees(Vector &A, Vector &result);
	static void Radians(Vector &A, Vector &result);
	static void Exp(Vector &A, Vector &result);
	static void Sqrt(Vector &A, Vector &result);
	static void Ln(Vector &A, Vector &result);
	static void Log10(Vector &A, Vector &result);
	static void Log2(Vector &A, Vector &result);
	static void Sign(Vector &A, Vector &result);
	static void Pow(Vector &A, Vector &B, Vector &result);

	static void Sin(Vector &A, Vector &result);
	static void Cos(Vector &A, Vector &result);
	static void Tan(Vector &A, Vector &result);
	static void ASin(Vector &A, Vector &result);
	static void ACos(Vector &A, Vector &result);
	static void ATan(Vector &A, Vector &result);
	static void ATan2(Vector &A, Vector &B, Vector &result);

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
	static void GenerateSequence(Vector &result, int64_t start = 0, int64_t increment = 1);
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
	static void ExecType(Vector &vector, FUNC &&fun, index_t offset = 0, index_t limit = 0) {
		auto data = (T *)vector.data;
		VectorOperations::Exec(
		    vector, [&](index_t i, index_t k) { fun(data[i], i, k); }, offset, limit);
	}
	//! NAryExec handles NULL values, sel_vector and count in the presence of potential constants
	template<bool HANDLE_NULLS>
	static void NAryExec(index_t N, Vector *vectors[], index_t multipliers[], Vector &result) {
		// initialize result to a constant (no sel_vector, count = 1)
		result.sel_vector = nullptr;
		result.count = 1;
		for(index_t i = 0; i < N; i++) {
			// for every vector, check if it is a constant
			if (vectors[i]->IsConstant()) {
				// if it is a constant, we set the index multiplier to 0
				// this ensures we always fetch the first element
				multipliers[i] = 0;
				if (HANDLE_NULLS && vectors[i]->nullmask[0]) {
					// if there is a constant NULL, we set the entire result to NULL
					result.nullmask.set();
				}
			} else {
				// if it is not a constant, we set the multiplier to 1
				// we set the result sel_vector/count to the count of this vector
				multipliers[i] = 1;
				result.sel_vector = vectors[i]->sel_vector;
				result.count = vectors[i]->count;
				if (HANDLE_NULLS) {
					// if we are handling nulls here, we OR this nullmask together with the result
					result.nullmask |= vectors[i]->nullmask;
				}
			}
		}
	}

	template <typename TA, typename TR, class FUNC, bool SKIP_NULLS = std::is_same<TR, const char*>()> static void UnaryExec(Vector &a, Vector &result, FUNC &&fun) {
		auto adata = (TA*) a.data;
		auto rdata = (TR*) result.data;
		result.sel_vector = a.sel_vector;
		result.count = a.count;
		result.nullmask = a.nullmask;
		if (SKIP_NULLS) {
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				if (result.nullmask[i]) {
					return;
				}
				rdata[i] = fun(adata[i]);
			});
		} else {
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				rdata[i] = fun(adata[i]);
			});
		}
	}
	template <typename TA, typename TB, typename TR, class FUNC, bool SKIP_NULLS = true, bool HANDLE_NULLS=true> static void BinaryExec(Vector &a, Vector &b, Vector &result, FUNC &&fun) {
		Vector *vectors[2] = {&a, &b};
		index_t multipliers[2];
		VectorOperations::NAryExec<HANDLE_NULLS>(2, vectors, multipliers, result);

		auto adata = (TA*) a.data;
		auto bdata = (TB*) b.data;
		auto rdata = (TR*) result.data;
		VectorOperations::Exec(result, [&](index_t i, index_t k) {
			if (SKIP_NULLS && result.nullmask[i]) {
				return;
			}
			rdata[i] = fun(adata[multipliers[0] * i], bdata[multipliers[1] * i], i);
		});
	}
	template <typename TA, typename TB, typename TC, typename TR, class FUNC, bool SKIP_NULLS = true, bool HANDLE_NULLS=true> static void TernaryExec(Vector &a, Vector &b, Vector &c, Vector &result, FUNC &&fun) {
		Vector *vectors[3] = {&a, &b, &c};
		index_t multipliers[3];
		VectorOperations::NAryExec<HANDLE_NULLS>(3, vectors, multipliers, result);

		auto adata = (TA*) a.data;
		auto bdata = (TB*) b.data;
		auto cdata = (TC*) c.data;
		auto rdata = (TR*) result.data;
		VectorOperations::Exec(result, [&](index_t i, index_t k) {
			if (SKIP_NULLS && result.nullmask[i]) {
				return;
			}
			rdata[i] = fun(adata[multipliers[0] * i], bdata[multipliers[1] * i], cdata[multipliers[2] * i], i);
		});
	}

	template <class FUNC> static void MultiaryExec(Vector inputs[], int input_count, Vector &result, FUNC &&fun) {
		result.sel_vector = nullptr;
		result.count = 1;
		vector<index_t> mul(input_count, 0);

		for (int i = 0; i < input_count; i++) {
			auto &input = inputs[i];
			if (!input.IsConstant()) {
				result.sel_vector = input.sel_vector;
				result.count = input.count;
			}
			mul[i] = input.IsConstant() ? 0 : 1;
		}
		VectorOperations::Exec(result, [&](index_t i, index_t k) { fun(mul, i); });
	}
};
} // namespace duckdb
