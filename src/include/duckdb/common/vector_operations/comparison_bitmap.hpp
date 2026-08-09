//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/comparison_bitmap.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/autovec.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <type_traits>

namespace duckdb {
// Fixed-width types the flat compare kernels handle; the predicate and dispatch below are generated from this list.
#define DUCKDB_BITMAP_CMP_TYPES(X)                                                                                     \
	X(INT8, int8_t)                                                                                                    \
	X(INT16, int16_t)                                                                                                  \
	X(INT32, int32_t)                                                                                                  \
	X(INT64, int64_t)                                                                                                  \
	X(UINT8, uint8_t) X(UINT16, uint16_t) X(UINT32, uint32_t) X(UINT64, uint64_t) X(FLOAT, float) X(DOUBLE, double)
inline bool BitmapCmpTypeSupported(PhysicalType pt) {
	switch (pt) {
#define DUCKDB_BITMAP_CMP_CASE(ENUM, TYPE) case PhysicalType::ENUM:
		DUCKDB_BITMAP_CMP_TYPES(DUCKDB_BITMAP_CMP_CASE)
#undef DUCKDB_BITMAP_CMP_CASE
		return true;
	default:
		return false;
	}
}
DUCKDB_AUTOVEC_TARGET inline void AndValidityIntoBitmap(const validity_t *validity, idx_t count,
                                                        validity_t *__restrict bitmap) {
	if (!validity) {
		return;
	}
	const idx_t nwords = ValidityMask::EntryCount(count);
	DUCKDB_UNROLL_LOOP
	for (idx_t w = 0; w < nwords; w++) {
		bitmap[w] &= validity[w];
	}
}
//! Compare into one 0/1 byte per row, 64 rows at a time; the compiler narrows the compare lanes to bytes by
//! itself, and BoolsToBits packs the group into a bitmap word.
template <class T, class OP, bool COL>
DUCKDB_AUTOVEC_TARGET static inline void CmpMaskToBitmap(const T *__restrict a, const T *__restrict b, T constant,
                                                         idx_t count, validity_t *__restrict bitmap) {
	for (idx_t i = 0; i < count; i += 64) {
		uint8_t bytes[64];
		const idx_t n = count - i < 64 ? count - i : 64;
		for (idx_t j = 0; j < n; j++) {
			bytes[j] = uint8_t(OP::Operation(a[i + j], COL ? b[i + j] : constant));
		}
		if (n < 64) {
			std::memset(bytes + n, 0, 64 - n); // only a ragged last group needs zeroing
		}
		bitmap[i / 64] = BoolsToBits(bytes);
	}
}
template <class T>
struct CmpEq {
	static inline bool Operation(T a, T b) { // bitwise, not short-circuit: the loop has to stay vectorizable
		if constexpr (std::is_floating_point<T>::value) {
			return ((a != a) & (b != b)) | (a == b); // total order: NaN == NaN
		} else {
			return a == b;
		}
	}
};
template <class T>
struct CmpGt {
	static inline bool Operation(T a, T b) {
		if constexpr (std::is_floating_point<T>::value) {
			return (b == b) & ((a != a) | (a > b)); // total order: NaN is largest
		} else {
			return a > b;
		}
	}
};
//! The remaining comparisons are CmpEq/CmpGt with the operands swapped and/or the result negated.
template <class BASE, bool SWAP, bool NEGATE>
struct CmpAdapt {
	template <class A>
	static inline bool Operation(A a, A b) {
		return NEGATE ^ (SWAP ? BASE::Operation(b, a) : BASE::Operation(a, b));
	}
};
template <class T>
using CmpNe = CmpAdapt<CmpEq<T>, false, true>;
template <class T>
using CmpLt = CmpAdapt<CmpGt<T>, true, false>;
template <class T>
using CmpGe = CmpAdapt<CmpGt<T>, true, true>;
template <class T>
using CmpLe = CmpAdapt<CmpGt<T>, false, true>;
template <class T, class FN>
inline void DispatchBitmapCmpOp(ExpressionType op, FN &&fn) { // one switch shared by const/col paths
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		return fn(CmpEq<T> {});
	case ExpressionType::COMPARE_NOTEQUAL:
		return fn(CmpNe<T> {});
	case ExpressionType::COMPARE_LESSTHAN:
		return fn(CmpLt<T> {});
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return fn(CmpLe<T> {});
	case ExpressionType::COMPARE_GREATERTHAN:
		return fn(CmpGt<T> {});
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return fn(CmpGe<T> {});
	default:
		throw InternalException("Unsupported comparison for bitmap filter");
	}
}
template <class FN>
inline void DispatchBitmapType(PhysicalType pt, FN &&fn) {
	switch (pt) {
#define DUCKDB_BITMAP_CMP_CASE(ENUM, TYPE)                                                                             \
	case PhysicalType::ENUM:                                                                                           \
		return fn(TYPE {});
		DUCKDB_BITMAP_CMP_TYPES(DUCKDB_BITMAP_CMP_CASE)
#undef DUCKDB_BITMAP_CMP_CASE
	default:
		throw InternalException("Unsupported type for bitmap select");
	}
}
//! Typed flat comparison into the bytemap; a null rdata_p means the constant path, a null validity means no NULLs.
template <class ConstGetter>
DUCKDB_AUTOVEC_TARGET inline void DispatchFlatCmpToBitmap(PhysicalType pt, ExpressionType op, const_data_ptr_t ldata_p,
                                                          const_data_ptr_t rdata_p, idx_t count,
                                                          const validity_t *lvalidity, const validity_t *rvalidity,
                                                          validity_t *__restrict bitmap, ConstGetter get_const) {
	DispatchBitmapType(pt, [&](auto tag) {
		using T = decltype(tag);
		const auto *ldata = reinterpret_cast<const T *>(ldata_p);
		const auto *rdata = rdata_p ? reinterpret_cast<const T *>(rdata_p) : ldata;
		const auto constant = get_const(tag);
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			if (rdata_p) { // one branch per dispatch, not per value
				CmpMaskToBitmap<T, decltype(cmp), true>(ldata, rdata, constant, count, bitmap);
			} else {
				CmpMaskToBitmap<T, decltype(cmp), false>(ldata, rdata, constant, count, bitmap);
			}
			AndValidityIntoBitmap(lvalidity, count, bitmap);
			AndValidityIntoBitmap(rvalidity, count, bitmap);
		});
	});
}
DUCKDB_AUTOVEC_TARGET inline idx_t BitmapPopcount(const validity_t *bitmap, idx_t count) { // count selected rows
	const idx_t nwords = ValidityMask::EntryCount(count);
	idx_t total = 0;
	DUCKDB_UNROLL_LOOP
	for (idx_t w = 0; w < nwords; w++) {
		total += CountOnes<validity_t>::Count(bitmap[w]);
	}
	return total;
}

} // namespace duckdb
