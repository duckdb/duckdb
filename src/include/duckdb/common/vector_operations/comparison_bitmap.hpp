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
#include "duckdb/common/vector_size.hpp"

#include <type_traits>

namespace duckdb {

//! Physical types the flat-column comparison-to-bitmap kernels (and the autovec arithmetic path) support.
inline bool BitmapCmpTypeSupported(PhysicalType pt) {
	switch (pt) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

inline bool BitmapCmpOpSupported(ExpressionType op) {
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

inline bool PreferSelectionGather(idx_t active_tuples, idx_t child_len, idx_t type_size) {
	return active_tuples * STANDARD_VECTOR_SIZE < child_len * (50 * type_size);
}

//! Pack a byte-per-row 0/1 array into a bit-per-row bitmap (validity_t word layout), zeroing the tail word.
DUCKDB_AUTOVEC_TARGET inline void PackBoolsToBitmap(const uint8_t *cmp, idx_t count, validity_t *__restrict bitmap) {
	auto out = reinterpret_cast<uint8_t *>(bitmap);
	const idx_t full = count / 8;
	for (idx_t b = 0; b < full; b++) {
		const auto c = cmp + b * 8;
		out[b] = static_cast<uint8_t>(c[0] | (c[1] << 1) | (c[2] << 2) | (c[3] << 3) | (c[4] << 4) | (c[5] << 5) |
		                              (c[6] << 6) | (c[7] << 7));
	}
	const idx_t nwords = (count + 63) / 64;
	for (idx_t i = full; i < nwords * 8; i++) {
		out[i] = 0;
	}
	if (full * 8 < count) {
		uint8_t tail = 0;
		for (idx_t i = full * 8; i < count; i++) {
			tail = static_cast<uint8_t>(tail | (cmp[i] << (i - full * 8)));
		}
		out[full] = tail;
	}
}

DUCKDB_AUTOVEC_TARGET inline void AndValidityIntoBitmap(const validity_t *validity, idx_t count,
                                                        validity_t *__restrict bitmap) {
	if (!validity) {
		return;
	}
	const idx_t nwords = (count + 63) / 64;
	for (idx_t w = 0; w < nwords; w++) {
		bitmap[w] &= validity[w];
	}
}

#if DUCKDB_AUTOVEC && defined(__x86_64__)
//! 32 comparison bits for 32 consecutive rows, without ISA-specific builtins: vector compares give 0/-1 lanes,
//! AND with per-lane bit weights, then an OR-reduction (shufflevector tree) collapses them into the mask word.
typedef uint8_t duckdb_cmp_u8x32 __attribute__((vector_size(32)));
typedef uint32_t duckdb_cmp_u32x8 __attribute__((vector_size(32)));
typedef uint64_t duckdb_cmp_u64x4 __attribute__((vector_size(32)));

DUCKDB_AUTOVEC_TARGET inline uint32_t CmpMaskHorOr(duckdb_cmp_u32x8 v) {
	v |= __builtin_shufflevector(v, v, 4, 5, 6, 7, 4, 5, 6, 7);
	v |= __builtin_shufflevector(v, v, 2, 3, 2, 3, 2, 3, 2, 3);
	v |= __builtin_shufflevector(v, v, 1, 1, 1, 1, 1, 1, 1, 1);
	return v[0];
}

//! One 0/-1 mask byte per row -> 32 bits: nibble weights per u32 lane, fold, position by lane, OR-reduce.
DUCKDB_AUTOVEC_TARGET inline uint32_t CmpMaskBytes(duckdb_cmp_u8x32 p) {
	const duckdb_cmp_u8x32 wb = {1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8,
	                             1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8, 1, 2, 4, 8};
	duckdb_cmp_u32x8 v = (duckdb_cmp_u32x8)(p & wb);
	v = v | (v >> 16);
	v = (v | (v >> 8)) & 0xF;
	v <<= duckdb_cmp_u32x8 {0, 4, 8, 12, 16, 20, 24, 28};
	return CmpMaskHorOr(v);
}

template <class T, class OP, bool COL>
DUCKDB_AUTOVEC_TARGET static inline uint32_t CmpMask32(const T *a, const T *b, T constant) {
	typedef T V __attribute__((vector_size(32)));
	constexpr std::size_t LANES = 32 / sizeof(T);
	V y {};
	if constexpr (!COL) {
		y = V {} + constant;
	}
	if constexpr (sizeof(T) <= 2) {
		V x0, y0 = y;
		std::memcpy(&x0, a, 32);
		if constexpr (COL) {
			std::memcpy(&y0, b, 32);
		}
		duckdb_cmp_u8x32 m0 = (duckdb_cmp_u8x32)OP::Apply(x0, y0);
		if constexpr (sizeof(T) == 1) {
			return CmpMaskBytes(m0);
		}
		V x1, y1 = y;
		std::memcpy(&x1, a + 16, 32);
		if constexpr (COL) {
			std::memcpy(&y1, b + 16, 32);
		}
		duckdb_cmp_u8x32 m1 = (duckdb_cmp_u8x32)OP::Apply(x1, y1);
		// 16-bit lanes: keep one mask byte per lane (both bytes are equal), giving 32 mask bytes
		return CmpMaskBytes(__builtin_shufflevector(m0, m1, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30,
		                                            32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62));
	} else if constexpr (sizeof(T) == 4) {
		duckdb_cmp_u32x8 acc {};
		for (std::size_t k = 0; k < 4; k++) {
			V x, yk = y;
			std::memcpy(&x, a + k * LANES, 32);
			if constexpr (COL) {
				std::memcpy(&yk, b + k * LANES, 32);
			}
			acc |= (duckdb_cmp_u32x8)OP::Apply(x, yk) & (duckdb_cmp_u32x8 {1, 2, 4, 8, 16, 32, 64, 128} << (k * 8));
		}
		return CmpMaskHorOr(acc);
	} else {
		duckdb_cmp_u64x4 acc {};
		for (std::size_t k = 0; k < 8; k++) {
			V x, yk = y;
			std::memcpy(&x, a + k * LANES, 32);
			if constexpr (COL) {
				std::memcpy(&yk, b + k * LANES, 32);
			}
			acc |= (duckdb_cmp_u64x4)OP::Apply(x, yk) & (duckdb_cmp_u64x4 {1, 2, 4, 8} << (k * 4));
		}
		acc |= __builtin_shufflevector(acc, acc, 2, 3, 2, 3);
		acc |= __builtin_shufflevector(acc, acc, 1, 1, 1, 1);
		return uint32_t(acc[0]);
	}
}

//! Whole-word compare-to-bitmap via CmpMask32; the sub-word tail is done scalar. Returns validity AND-ed in.
template <class T, class OP, bool COL>
DUCKDB_AUTOVEC_TARGET static inline void CmpMaskToBitmap(const T *__restrict a, const T *__restrict b, T constant,
                                                         idx_t count, validity_t *__restrict bitmap) {
	idx_t i = 0;
	for (; i + 64 <= count; i += 64) {
		bitmap[i / 64] = validity_t(CmpMask32<T, OP, COL>(a + i, b + i, constant)) |
		                 validity_t(CmpMask32<T, OP, COL>(a + i + 32, b + i + 32, constant)) << 32;
	}
	if (i < count) {
		validity_t word = 0;
		for (idx_t j = i; j < count; j++) {
			word |= validity_t(OP::Operation(a[j], COL ? b[j] : constant)) << (j - i);
		}
		bitmap[i / 64] = word;
	}
}
#endif

//! Branchless `data[i] <cmp> constant` over a contiguous flat array, producing a packed result bitmap
//! (one bit per row, validity_t layout). NULLs are removed by AND-ing the validity mask afterwards.
template <class T, class OP>
DUCKDB_AUTOVEC_TARGET inline void NarrowCmpToBitmap(const T *__restrict data, T constant, idx_t count,
                                                    const validity_t *validity, validity_t *__restrict bitmap) {
#if DUCKDB_AUTOVEC && defined(__x86_64__)
	if constexpr (std::is_integral<T>::value) {
		CmpMaskToBitmap<T, OP, false>(data, data, constant, count, bitmap);
		AndValidityIntoBitmap(validity, count, bitmap);
		return;
	}
#endif
	uint8_t cmp[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		cmp[i] = OP::Operation(data[i], constant);
	}
	PackBoolsToBitmap(cmp, count, bitmap);
	AndValidityIntoBitmap(validity, count, bitmap);
}

//! Branchless `ldata[i] <cmp> rdata[i]` over two contiguous flat arrays. A row is NULL (excluded) if either side
//! is NULL, so both validity masks are AND-ed in.
template <class T, class OP>
DUCKDB_AUTOVEC_TARGET inline void NarrowColCmpToBitmap(const T *__restrict ldata, const T *__restrict rdata,
                                                       idx_t count, const validity_t *lvalidity,
                                                       const validity_t *rvalidity, validity_t *__restrict bitmap) {
#if DUCKDB_AUTOVEC && defined(__x86_64__)
	if constexpr (std::is_integral<T>::value) {
		CmpMaskToBitmap<T, OP, true>(ldata, rdata, T(0), count, bitmap);
		AndValidityIntoBitmap(lvalidity, count, bitmap);
		AndValidityIntoBitmap(rvalidity, count, bitmap);
		return;
	}
#endif
	uint8_t cmp[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		cmp[i] = OP::Operation(ldata[i], rdata[i]);
	}
	PackBoolsToBitmap(cmp, count, bitmap);
	AndValidityIntoBitmap(lvalidity, count, bitmap);
	AndValidityIntoBitmap(rvalidity, count, bitmap);
}

// Comparison primitives. For floats this is DuckDB's total order (NaN == NaN, NaN > every
// non-NaN incl. +inf, +-0 equal); `x != x` is the branchless isnan. The other ops derive from these.
template <class T>
struct CmpEq {
	static inline bool Operation(T a, T b) {
		if constexpr (std::is_floating_point<T>::value) {
			return ((a != a) && (b != b)) || (a == b);
		} else {
			return a == b;
		}
	}
	// vector form (integers only): 0/-1 lanes for the movemask fastpath
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline V Apply(V a, V b) {
		return a == b;
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
	DUCKDB_AUTOVEC_TARGET static inline V Apply(V a, V b) {
		return a > b;
	}
};
template <class T>
struct CmpNe {
	static inline bool Operation(T a, T b) {
		return !CmpEq<T>::Operation(a, b);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline V Apply(V a, V b) {
		return a != b;
	}
};
template <class T>
struct CmpLt {
	static inline bool Operation(T a, T b) {
		return CmpGt<T>::Operation(b, a);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline V Apply(V a, V b) {
		return a < b;
	}
};
template <class T>
struct CmpGe {
	static inline bool Operation(T a, T b) {
		return !CmpGt<T>::Operation(b, a);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline V Apply(V a, V b) {
		return a >= b;
	}
};
template <class T>
struct CmpLe {
	static inline bool Operation(T a, T b) {
		return !CmpGt<T>::Operation(a, b);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline V Apply(V a, V b) {
		return a <= b;
	}
};

//! Invoke fn(OP{}) with the Cmp<T> comparison functor selected by `op`. Shared by the col-vs-const and col-vs-col
//! paths so the six-way op switch lives in one place.
template <class T, class FN>
inline void DispatchBitmapCmpOp(ExpressionType op, FN &&fn) {
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

//! Invoke fn(T{}) with the C++ type selected by `pt` (the bitmap-supported fixed-width types). The result bitmap is
//! a fixed STANDARD_VECTOR_SIZE buffer, so `count` is guarded here (in release too, not just an assert: a silent heap
//! overflow is far worse than a clean error). Shared by every flat bitmap-comparison entry point.
template <class FN>
inline void DispatchBitmapType(PhysicalType pt, idx_t count, FN &&fn) {
	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("bitmap comparison called with count > STANDARD_VECTOR_SIZE");
	}
	switch (pt) {
	case PhysicalType::INT8:
		return fn(int8_t {});
	case PhysicalType::INT16:
		return fn(int16_t {});
	case PhysicalType::INT32:
		return fn(int32_t {});
	case PhysicalType::INT64:
		return fn(int64_t {});
	case PhysicalType::UINT8:
		return fn(uint8_t {});
	case PhysicalType::UINT16:
		return fn(uint16_t {});
	case PhysicalType::UINT32:
		return fn(uint32_t {});
	case PhysicalType::UINT64:
		return fn(uint64_t {});
	case PhysicalType::FLOAT:
		return fn(float {});
	case PhysicalType::DOUBLE:
		return fn(double {});
	default:
		throw InternalException("Unsupported type for bitmap select");
	}
}

//! `flat[i] <op> constant` over a flat column into a bitmap. The typed scalar is produced by get_const(T{}), so each
//! caller sources the constant from its own representation with no runtime overhead. Check BitmapCmpTypeSupported
//! first.
template <class ConstGetter>
inline void DispatchFlatCmpToBitmap(PhysicalType pt, ExpressionType op, const Vector &flat, idx_t count,
                                    const validity_t *validity, validity_t *__restrict bitmap, ConstGetter get_const) {
	DispatchBitmapType(pt, count, [&](auto tag) {
		using T = decltype(tag);
		const auto *data = FlatVector::GetData<T>(flat);
		const auto constant = get_const(tag);
		DispatchBitmapCmpOp<T>(
		    op, [&](auto cmp) { NarrowCmpToBitmap<T, decltype(cmp)>(data, constant, count, validity, bitmap); });
	});
}

//! Fill the first `count` rows of a bitmap with a constant result, folding in the validity mask. Used by the FOR
//! column-vs-constant path when range analysis proves the comparison is uniformly true/false.
inline void WriteConstantBitmap(bool value, idx_t count, const validity_t *validity, validity_t *__restrict bitmap) {
	const idx_t nwords = (count + 63) / 64;
	if (!value) {
		for (idx_t w = 0; w < nwords; w++) {
			bitmap[w] = 0;
		}
		return;
	}
	for (idx_t w = 0; w < nwords; w++) {
		bitmap[w] = ~validity_t(0);
	}
	if (count % 64) {
		bitmap[nwords - 1] &= (validity_t(1) << (count % 64)) - 1;
	}
	if (validity) {
		for (idx_t w = 0; w < nwords; w++) {
			bitmap[w] &= validity[w];
		}
	}
}

//! Number of set bits in the first `count` rows of a packed bitmap (validity_t words).
DUCKDB_AUTOVEC_TARGET inline idx_t BitmapPopcount(const validity_t *bitmap, idx_t count) {
	const idx_t nwords = (count + 63) / 64;
	idx_t total = 0;
	for (idx_t w = 0; w < nwords; w++) {
		total += CountOnes<validity_t>::Count(bitmap[w]);
	}
	return total;
}

} // namespace duckdb
