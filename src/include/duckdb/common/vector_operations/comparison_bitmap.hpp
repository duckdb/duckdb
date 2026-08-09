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
inline uint64_t ValidityBitsToBytes(uint8_t bits) { // 8 validity bits -> 8 bytes, byte i = bit i (little-endian)
	const uint64_t lanes = (uint64_t(bits) * 0x0101010101010101ULL) & 0x8040201008040201ULL;
	return ((lanes + 0x7F7F7F7F7F7F7F7FULL) >> 7) & 0x0101010101010101ULL;
}
DUCKDB_AUTOVEC_TARGET inline void AndValidityIntoBytemap(const validity_t *validity, idx_t count,
                                                         uint8_t *__restrict bytemap) { // validity is bit-per-row
	if (!validity) {
		return;
	}
	for (idx_t i = 0; i < count; i += 8) {
		uint64_t chunk;
		std::memcpy(&chunk, bytemap + i, 8);
		chunk &= ValidityBitsToBytes(uint8_t(validity[i / 64] >> (i & 63)));
		std::memcpy(bytemap + i, &chunk, 8);
	}
}
//! Compare into one 0/1 byte per row; the compiler narrows the compare lanes to bytes by itself.
template <class T, class OP, bool COL>
DUCKDB_AUTOVEC_TARGET static inline void CmpToBytemap(const T *__restrict a, const T *__restrict b, T constant,
                                                      idx_t count, uint8_t *__restrict bytemap) {
	for (idx_t i = 0; i < count; i++) {
		bytemap[i] = uint8_t(OP::Operation(a[i], COL ? b[i] : constant));
	}
	std::memset(bytemap + count, 0, ((count + 63) & ~idx_t(63)) - count); // zero the tail: consumers read whole groups
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
                                                          uint8_t *__restrict bytemap, ConstGetter get_const) {
	DispatchBitmapType(pt, [&](auto tag) {
		using T = decltype(tag);
		const auto *ldata = reinterpret_cast<const T *>(ldata_p);
		const auto *rdata = rdata_p ? reinterpret_cast<const T *>(rdata_p) : ldata;
		const auto constant = get_const(tag);
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			if (rdata_p) { // one branch per dispatch, not per value
				CmpToBytemap<T, decltype(cmp), true>(ldata, rdata, constant, count, bytemap);
			} else {
				CmpToBytemap<T, decltype(cmp), false>(ldata, rdata, constant, count, bytemap);
			}
			AndValidityIntoBytemap(lvalidity, count, bytemap);
			AndValidityIntoBytemap(rvalidity, count, bytemap);
		});
	});
}
DUCKDB_AUTOVEC_TARGET inline idx_t BitmapPopcount(const uint8_t *bytemap, idx_t count) { // count selected rows
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		total += bytemap[i];
	}
	return total;
}

} // namespace duckdb
