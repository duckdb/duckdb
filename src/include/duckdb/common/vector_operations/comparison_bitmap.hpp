//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/comparison_bitmap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_size.hpp"

#include <type_traits>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

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

//! Branchless `data[i] <cmp> constant` over a contiguous flat array, producing a packed result bitmap
//! (one bit per row, validity_t layout). NULLs are removed by AND-ing the validity mask afterwards.
template <class T, class OP>
inline void NarrowCmpToBitmap(const T *__restrict data, T constant, idx_t count, const validity_t *validity,
                              validity_t *__restrict bitmap) {
	uint8_t cmp[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		cmp[i] = OP::Operation(data[i], constant);
	}

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

	if (validity) {
		for (idx_t w = 0; w < nwords; w++) {
			bitmap[w] &= validity[w];
		}
	}
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

template <class T>
inline void DispatchCmpToBitmap(ExpressionType op, const T *__restrict data, T constant, idx_t count,
                                const validity_t *validity, validity_t *__restrict bitmap) {
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		return NarrowCmpToBitmap<T, CmpEq<T>>(data, constant, count, validity, bitmap);
	case ExpressionType::COMPARE_NOTEQUAL:
		return NarrowCmpToBitmap<T, CmpNe<T>>(data, constant, count, validity, bitmap);
	case ExpressionType::COMPARE_LESSTHAN:
		return NarrowCmpToBitmap<T, CmpLt<T>>(data, constant, count, validity, bitmap);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return NarrowCmpToBitmap<T, CmpLe<T>>(data, constant, count, validity, bitmap);
	case ExpressionType::COMPARE_GREATERTHAN:
		return NarrowCmpToBitmap<T, CmpGt<T>>(data, constant, count, validity, bitmap);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return NarrowCmpToBitmap<T, CmpGe<T>>(data, constant, count, validity, bitmap);
	default:
		throw InternalException("Unsupported comparison for bitmap filter");
	}
}

//! `flat[i] <op> constant` over a flat column into a bitmap. The typed scalar is produced by get_const(T{}),
//! so each caller sources the constant from its own representation (a constant Vector, a bound Value, ...)
//! with no runtime overhead. Callers must first check the type via BitmapCmpTypeSupported.
template <class ConstGetter>
inline void DispatchFlatCmpToBitmap(PhysicalType pt, ExpressionType op, const Vector &flat, idx_t count,
                                    const validity_t *validity, validity_t *__restrict bitmap, ConstGetter get_const) {
	switch (pt) {
	case PhysicalType::INT8:
		return DispatchCmpToBitmap<int8_t>(op, FlatVector::GetData<int8_t>(flat), get_const(int8_t {}), count, validity,
		                                   bitmap);
	case PhysicalType::INT16:
		return DispatchCmpToBitmap<int16_t>(op, FlatVector::GetData<int16_t>(flat), get_const(int16_t {}), count,
		                                    validity, bitmap);
	case PhysicalType::INT32:
		return DispatchCmpToBitmap<int32_t>(op, FlatVector::GetData<int32_t>(flat), get_const(int32_t {}), count,
		                                    validity, bitmap);
	case PhysicalType::INT64:
		return DispatchCmpToBitmap<int64_t>(op, FlatVector::GetData<int64_t>(flat), get_const(int64_t {}), count,
		                                    validity, bitmap);
	case PhysicalType::UINT8:
		return DispatchCmpToBitmap<uint8_t>(op, FlatVector::GetData<uint8_t>(flat), get_const(uint8_t {}), count,
		                                    validity, bitmap);
	case PhysicalType::UINT16:
		return DispatchCmpToBitmap<uint16_t>(op, FlatVector::GetData<uint16_t>(flat), get_const(uint16_t {}), count,
		                                     validity, bitmap);
	case PhysicalType::UINT32:
		return DispatchCmpToBitmap<uint32_t>(op, FlatVector::GetData<uint32_t>(flat), get_const(uint32_t {}), count,
		                                     validity, bitmap);
	case PhysicalType::UINT64:
		return DispatchCmpToBitmap<uint64_t>(op, FlatVector::GetData<uint64_t>(flat), get_const(uint64_t {}), count,
		                                     validity, bitmap);
	case PhysicalType::FLOAT:
		return DispatchCmpToBitmap<float>(op, FlatVector::GetData<float>(flat), get_const(float {}), count, validity,
		                                  bitmap);
	case PhysicalType::DOUBLE:
		return DispatchCmpToBitmap<double>(op, FlatVector::GetData<double>(flat), get_const(double {}), count, validity,
		                                   bitmap);
	default:
		throw InternalException("Unsupported type for bitmap select");
	}
}

//! Number of set bits in the first `count` rows of a packed bitmap (validity_t words).
inline idx_t BitmapPopcount(const validity_t *bitmap, idx_t count) {
	const idx_t nwords = (count + 63) / 64;
	idx_t total = 0;
	for (idx_t w = 0; w < nwords; w++) {
#if defined(_MSC_VER)
		total += idx_t(__popcnt64(bitmap[w]));
#else
		total += idx_t(__builtin_popcountll(bitmap[w]));
#endif
	}
	return total;
}

} // namespace duckdb
