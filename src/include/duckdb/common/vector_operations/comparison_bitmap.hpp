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

// The fixed-width types the flat comparison-to-bitmap kernels handle: the predicate below and the
// dispatch further down are both generated from this list, so they cannot drift apart.
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

#if DUCKDB_AUTOVEC
template <class T, class OP, bool COL>
DUCKDB_AUTOVEC_TARGET static inline uint32_t CmpMask32(const T *a, const T *b, T constant) {
	typedef T V __attribute__((vector_size(32))); // compare lanes become 0/-1 mask lanes
	constexpr std::size_t LANES = 32 / sizeof(T);
	V y {};
	if constexpr (!COL) {
		y = V {} + constant;
	}
	if constexpr (sizeof(T) == 1) {
		V x;
		std::memcpy(&x, a, 32);
		if constexpr (COL) {
			std::memcpy(&y, b, 32);
		}
		return MoveMask((duckdb_av_u8x32)OP::Apply(x, y));
	} else if constexpr (sizeof(T) == 2) {
		V x0, x1, y0 = y, y1 = y;
		std::memcpy(&x0, a, 32);
		std::memcpy(&x1, a + 16, 32);
		if constexpr (COL) {
			std::memcpy(&y0, b, 32);
			std::memcpy(&y1, b + 16, 32);
		}
		return MoveMask((duckdb_av_u16x16)OP::Apply(x0, y0), (duckdb_av_u16x16)OP::Apply(x1, y1));
	} else { // 4/8-byte lanes: one movemask per 32-byte vector, contributing LANES bits each
		using M = typename std::conditional<sizeof(T) == 4, duckdb_av_u32x8, duckdb_av_u64x4>::type;
		uint32_t mask = 0;
		DUCKDB_UNROLL_LOOP
		for (std::size_t k = 0; k < 32 / LANES; k++) {
			V x, yk = y;
			std::memcpy(&x, a + k * LANES, 32);
			if constexpr (COL) {
				std::memcpy(&yk, b + k * LANES, 32);
			}
			mask |= MoveMask((M)OP::Apply(x, yk)) << (k * LANES);
		}
		return mask;
	}
}

#endif

template <class T, class OP, bool COL>
DUCKDB_AUTOVEC_TARGET static inline void CmpMaskToBitmap(const T *__restrict a, const T *__restrict b, T constant,
                                                         idx_t count, validity_t *__restrict bitmap) {
	idx_t i = 0;
#if DUCKDB_AUTOVEC
	for (; i + 64 <= count; i += 64) { // whole words via movemask
		bitmap[i / 64] = validity_t(CmpMask32<T, OP, COL>(a + i, b + i, constant)) |
		                 validity_t(CmpMask32<T, OP, COL>(a + i + 32, b + i + 32, constant)) << 32;
	}
#endif
	for (; i < count; i += 64) { // remaining words scalar (all words when no autovec)
		validity_t word = 0;
		const idx_t n = count - i < 64 ? count - i : 64;
		DUCKDB_UNROLL_LOOP
		for (idx_t j = 0; j < n; j++) {
			word |= validity_t(OP::Operation(a[i + j], COL ? b[i + j] : constant)) << j;
		}
		bitmap[i / 64] = word;
	}
}

template <class T>
struct CmpEq {
	static inline bool Operation(T a, T b) {
		if constexpr (std::is_floating_point<T>::value) {
			return ((a != a) && (b != b)) || (a == b);
		} else {
			return a == b;
		}
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
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
			return (b == b) && ((a != a) || (a > b));
		} else {
			return a > b;
		}
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
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
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
		if constexpr (NEGATE) {
			return ~(SWAP ? BASE::Apply(b, a) : BASE::Apply(a, b));
		} else {
			return SWAP ? BASE::Apply(b, a) : BASE::Apply(a, b);
		}
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

//! Typed flat comparison into a bitmap. A null rdata_p selects the constant path, where get_const supplies the
//! typed constant without a runtime cast; a null validity pointer means the side cannot be NULL.
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
