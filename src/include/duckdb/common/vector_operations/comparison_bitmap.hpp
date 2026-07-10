//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/comparison_bitmap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

//! Pack a byte-per-row 0/1 array into a bit-per-row bitmap (validity_t word layout), zeroing the tail word.
inline void PackBoolsToBitmap(const uint8_t *cmp, idx_t count, validity_t *__restrict bitmap) {
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

inline void AndValidityIntoBitmap(const validity_t *validity, idx_t count, validity_t *__restrict bitmap) {
	if (!validity) {
		return;
	}
	const idx_t nwords = (count + 63) / 64;
	for (idx_t w = 0; w < nwords; w++) {
		bitmap[w] &= validity[w];
	}
}

//! Branchless `data[i] <cmp> constant` over a contiguous flat array, producing a packed result bitmap
//! (one bit per row, validity_t layout). NULLs are removed by AND-ing the validity mask afterwards.
template <class T, class OP>
inline void NarrowCmpToBitmap(const T *__restrict data, T constant, idx_t count, const validity_t *validity,
                              validity_t *__restrict bitmap) {
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
inline void NarrowColCmpToBitmap(const T *__restrict ldata, const T *__restrict rdata, idx_t count,
                                 const validity_t *lvalidity, const validity_t *rvalidity,
                                 validity_t *__restrict bitmap) {
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
};
template <class T>
struct CmpNe {
	static inline bool Operation(T a, T b) {
		return !CmpEq<T>::Operation(a, b);
	}
};
template <class T>
struct CmpLt {
	static inline bool Operation(T a, T b) {
		return CmpGt<T>::Operation(b, a);
	}
};
template <class T>
struct CmpGe {
	static inline bool Operation(T a, T b) {
		return !CmpGt<T>::Operation(b, a);
	}
};
template <class T>
struct CmpLe {
	static inline bool Operation(T a, T b) {
		return !CmpGt<T>::Operation(a, b);
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

//! `left[i] <op> right[i]` over two flat columns of the same type into a bitmap. Check BitmapCmpTypeSupported first
//! and that both inputs are flat.
inline void DispatchFlatColCmpToBitmap(PhysicalType pt, ExpressionType op, const Vector &left, const Vector &right,
                                       idx_t count, const validity_t *lvalidity, const validity_t *rvalidity,
                                       validity_t *__restrict bitmap) {
	DispatchBitmapType(pt, count, [&](auto tag) {
		using T = decltype(tag);
		const auto *ldata = FlatVector::GetData<T>(left);
		const auto *rdata = FlatVector::GetData<T>(right);
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			NarrowColCmpToBitmap<T, decltype(cmp)>(ldata, rdata, count, lvalidity, rvalidity, bitmap);
		});
	});
}

//! Number of set bits in the first `count` rows of a packed bitmap (validity_t words).
inline idx_t BitmapPopcount(const validity_t *bitmap, idx_t count) {
	const idx_t nwords = (count + 63) / 64;
	idx_t total = 0;
	for (idx_t w = 0; w < nwords; w++) {
		total += CountOnes<validity_t>::Count(bitmap[w]);
	}
	return total;
}

} // namespace duckdb
