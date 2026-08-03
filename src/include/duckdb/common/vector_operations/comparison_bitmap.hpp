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

// Flat comparison-to-bitmap kernels support fixed-width autovec types.
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
	} else if constexpr (sizeof(T) == 4) {
		uint32_t mask = 0;
		for (std::size_t k = 0; k < 4; k++) {
			V x, yk = y;
			std::memcpy(&x, a + k * LANES, 32);
			if constexpr (COL) {
				std::memcpy(&yk, b + k * LANES, 32);
			}
			mask |= MoveMask((duckdb_av_u32x8)OP::Apply(x, yk)) << (k * 8);
		}
		return mask;
	} else {
		uint32_t mask = 0;
		for (std::size_t k = 0; k < 8; k++) {
			V x, yk = y;
			std::memcpy(&x, a + k * LANES, 32);
			if constexpr (COL) {
				std::memcpy(&yk, b + k * LANES, 32);
			}
			mask |= MoveMask((duckdb_av_u64x4)OP::Apply(x, yk)) << (k * 4);
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
template <class T>
struct CmpNe {
	static inline bool Operation(T a, T b) {
		return !CmpEq<T>::Operation(a, b);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
		return ~CmpEq<T>::Apply(a, b);
	}
};
template <class T>
struct CmpLt {
	static inline bool Operation(T a, T b) {
		return CmpGt<T>::Operation(b, a);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
		return CmpGt<T>::Apply(b, a);
	}
};
template <class T>
struct CmpGe {
	static inline bool Operation(T a, T b) {
		return !CmpGt<T>::Operation(b, a);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
		return ~CmpGt<T>::Apply(b, a);
	}
};
template <class T>
struct CmpLe {
	static inline bool Operation(T a, T b) {
		return !CmpGt<T>::Operation(a, b);
	}
	template <class V>
	DUCKDB_AUTOVEC_TARGET static inline auto Apply(V a, V b) {
		return ~CmpGt<T>::Apply(a, b);
	}
};

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
inline void DispatchBitmapType(PhysicalType pt, idx_t count, FN &&fn) {
	if (count > STANDARD_VECTOR_SIZE) { // result bitmap is one vector-sized scratch buffer
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

template <class ConstGetter>
DUCKDB_AUTOVEC_TARGET inline void DispatchFlatCmpToBitmap(PhysicalType pt, ExpressionType op, const_data_ptr_t data_p,
                                                          idx_t count, const validity_t *validity,
                                                          validity_t *__restrict bitmap, ConstGetter get_const) {
	DispatchBitmapType(pt, count, [&](auto tag) { // typed flat col/constant dispatch
		using T = decltype(tag);
		const auto *data = reinterpret_cast<const T *>(data_p);
		const auto constant = get_const(tag); // caller supplies typed constant without runtime cast
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			CmpMaskToBitmap<T, decltype(cmp), false>(data, data, constant, count, bitmap);
			AndValidityIntoBitmap(validity, count, bitmap);
		});
	});
}

DUCKDB_AUTOVEC_TARGET inline void DispatchFlatColCmpToBitmap(PhysicalType pt, ExpressionType op,
                                                             const_data_ptr_t ldata_p, const_data_ptr_t rdata_p,
                                                             idx_t count, const validity_t *lvalidity,
                                                             const validity_t *rvalidity,
                                                             validity_t *__restrict bitmap) {
	DispatchBitmapType(pt, count, [&](auto tag) { // typed flat col/constant dispatch
		using T = decltype(tag);
		const auto *ldata = reinterpret_cast<const T *>(ldata_p);
		const auto *rdata = reinterpret_cast<const T *>(rdata_p);
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			CmpMaskToBitmap<T, decltype(cmp), true>(ldata, rdata, T(0), count, bitmap);
			AndValidityIntoBitmap(lvalidity, count, bitmap);
			AndValidityIntoBitmap(rvalidity, count, bitmap);
		});
	});
}

DUCKDB_AUTOVEC_TARGET inline idx_t BitmapPopcount(const validity_t *bitmap, idx_t count) { // count selected rows
	const idx_t nwords = (count + 63) / 64;
	idx_t total = 0;
	for (idx_t w = 0; w < nwords; w++) {
		total += CountOnes<validity_t>::Count(bitmap[w]);
	}
	return total;
}

} // namespace duckdb
