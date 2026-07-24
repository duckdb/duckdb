//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_vector_arithmetic.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <type_traits>
#include <utility>

namespace duckdb {

// FOR arithmetic preserves narrow payloads for +, -, * when bounds prove a FOR result.
template <class OP>
struct FOROpTraits {
	static constexpr bool IS_SUPPORTED = false;
	static constexpr bool IS_ADD = false;
	static constexpr bool IS_SUBTRACT = false;
	static constexpr bool IS_MULTIPLY = false;
};

template <bool ADD, bool SUBTRACT, bool MULTIPLY, class TRY_OP, class EXEC_OP>
struct FOROpTraitBase {
	static constexpr bool IS_SUPPORTED = true;
	static constexpr bool IS_ADD = ADD;
	static constexpr bool IS_SUBTRACT = SUBTRACT;
	static constexpr bool IS_MULTIPLY = MULTIPLY;
	using TryOp = TRY_OP;
	using ExecOp = EXEC_OP;
};
template <>
struct FOROpTraits<AddOperator> : FOROpTraitBase<true, false, false, TryAddOperator, AddOperator> {};
template <>
struct FOROpTraits<AddOperatorOverflowCheck> : FOROpTraits<AddOperator> {};
template <>
struct FOROpTraits<DecimalAddOverflowCheck> : FOROpTraitBase<true, false, false, TryDecimalAdd, AddOperator> {};
template <>
struct FOROpTraits<SubtractOperator> : FOROpTraitBase<false, true, false, TrySubtractOperator, SubtractOperator> {};
template <>
struct FOROpTraits<SubtractOperatorOverflowCheck> : FOROpTraits<SubtractOperator> {};
template <>
struct FOROpTraits<DecimalSubtractOverflowCheck>
    : FOROpTraitBase<false, true, false, TryDecimalSubtract, SubtractOperator> {};
template <>
struct FOROpTraits<MultiplyOperator> : FOROpTraitBase<false, false, true, TryMultiplyOperator, MultiplyOperator> {};
template <>
struct FOROpTraits<MultiplyOperatorOverflowCheck> : FOROpTraits<MultiplyOperator> {};
template <>
struct FOROpTraits<DecimalMultiplyOverflowCheck>
    : FOROpTraitBase<false, false, true, TryDecimalMultiply, MultiplyOperator> {};

template <class OP, class T>
static bool FORBounds(T lmin, T lmax, T rmin, T rmax, T &result_min, T &result_max) {
	using TRY_OP = typename FOROpTraits<OP>::TryOp;
	if constexpr (FOROpTraits<OP>::IS_ADD) {
		return TRY_OP::Operation(lmin, rmin, result_min) && TRY_OP::Operation(lmax, rmax, result_max);
	} else if constexpr (FOROpTraits<OP>::IS_SUBTRACT) {
		return TRY_OP::Operation(lmin, rmax, result_min) && TRY_OP::Operation(lmax, rmin, result_max);
	} else {
		T values[4];
		if (!TRY_OP::Operation(lmin, rmin, values[0]) || !TRY_OP::Operation(lmin, rmax, values[1]) ||
		    !TRY_OP::Operation(lmax, rmin, values[2]) || !TRY_OP::Operation(lmax, rmax, values[3])) {
			return false;
		}
		result_min = values[0];
		result_max = values[0];
		for (idx_t i = 1; i < 4; i++) {
			result_min = MinValue(result_min, values[i]);
			result_max = MaxValue(result_max, values[i]);
		}
		return true;
	}
}

#ifndef DUCKDB_SMALLER_BINARY
using binary_buffer_kernel_t = void (*)(const BinaryBufferArgs &);

// Resolves the buffer-level kernels the registered arithmetic already instantiated: same-width,
// or a mixed unsigned pair whose wider side carries the result (in-register argument promotion
// widens the narrow side). No arithmetic loop exists on the FOR side at all.
template <class OP>
struct FORStandardExecutor {
	static PhysicalType MaxStored(PhysicalType a, PhysicalType b) {
		return GetTypeIdSize(a) >= GetTypeIdSize(b) ? a : b;
	}
	static LogicalType ViewType(PhysicalType stored) {
		switch (stored) {
		case PhysicalType::UINT8:
			return LogicalType::UTINYINT;
		case PhysicalType::UINT16:
			return LogicalType::USMALLINT;
		case PhysicalType::UINT32:
			return LogicalType::UINTEGER;
		default:
			return LogicalType::UBIGINT;
		}
	}
	static binary_buffer_kernel_t Kernel(PhysicalType lw, PhysicalType rw, PhysicalType res) {
		if (lw == rw) {
			D_ASSERT(lw == res);
			switch (res) {
			case PhysicalType::UINT8:
				return &BinaryExecutor::ExecuteBuffersStandard<uint8_t, uint8_t, uint8_t, OP>;
			case PhysicalType::UINT16:
				return &BinaryExecutor::ExecuteBuffersStandard<uint16_t, uint16_t, uint16_t, OP>;
			case PhysicalType::UINT32:
				return &BinaryExecutor::ExecuteBuffersStandard<uint32_t, uint32_t, uint32_t, OP>;
			default:
				return &BinaryExecutor::ExecuteBuffersStandard<uint64_t, uint64_t, uint64_t, OP>;
			}
		}
		D_ASSERT(res == MaxStored(lw, rw));
#define DUCKDB_FOR_MIXED_PAIR(LP, RP, TL, TR_, TRES)                                                                   \
	if (lw == PhysicalType::LP && rw == PhysicalType::RP) {                                                            \
		return &BinaryExecutor::ExecuteBuffersStandard<TL, TR_, TRES, OP>;                                              \
	}
		DUCKDB_FOR_MIXED_PAIR(UINT16, UINT8, uint16_t, uint8_t, uint16_t)
		DUCKDB_FOR_MIXED_PAIR(UINT8, UINT16, uint8_t, uint16_t, uint16_t)
		DUCKDB_FOR_MIXED_PAIR(UINT32, UINT8, uint32_t, uint8_t, uint32_t)
		DUCKDB_FOR_MIXED_PAIR(UINT8, UINT32, uint8_t, uint32_t, uint32_t)
		DUCKDB_FOR_MIXED_PAIR(UINT32, UINT16, uint32_t, uint16_t, uint32_t)
		DUCKDB_FOR_MIXED_PAIR(UINT16, UINT32, uint16_t, uint32_t, uint32_t)
		DUCKDB_FOR_MIXED_PAIR(UINT64, UINT8, uint64_t, uint8_t, uint64_t)
		DUCKDB_FOR_MIXED_PAIR(UINT8, UINT64, uint8_t, uint64_t, uint64_t)
		DUCKDB_FOR_MIXED_PAIR(UINT64, UINT16, uint64_t, uint16_t, uint64_t)
		DUCKDB_FOR_MIXED_PAIR(UINT16, UINT64, uint16_t, uint64_t, uint64_t)
		DUCKDB_FOR_MIXED_PAIR(UINT64, UINT32, uint64_t, uint32_t, uint64_t)
		DUCKDB_FOR_MIXED_PAIR(UINT32, UINT64, uint32_t, uint64_t, uint64_t)
#undef DUCKDB_FOR_MIXED_PAIR
		throw InternalException("Unsupported type pair for FOR arithmetic kernel");
	}
	//! Widen a payload to the result width when the result outgrows both operands (existing WidenToFlat)
	static data_ptr_t Operand(data_ptr_t data, PhysicalType stored, PhysicalType compute, idx_t count,
	                          unique_ptr<Vector> &scratch) {
		if (stored == compute) {
			return data;
		}
		if (!scratch) {
			scratch = make_uniq<Vector>(LogicalType::UBIGINT, idx_t(STANDARD_VECTOR_SIZE));
		}
		auto target = FlatVector::GetDataMutable(*scratch);
		FORVector::WidenToFlat(ViewType(compute), stored, data, target, *FlatVector::IncrementalSelectionVector(),
		                       count);
		return target;
	}
};
#endif

// FOR (op) constant: compute the result width once, then run the registered kernel on the narrow payload.
template <class DOMAIN_T, class OP>
static bool TryFORConstant(Vector &left, Vector &right, Vector &result, idx_t count,
                           buffer_ptr<DictionaryEntry> &dict_cache) {
	using TRAITS = FOROpTraits<OP>;
#ifdef DUCKDB_SMALLER_BINARY
	(void)left;
	(void)right;
	(void)result;
	(void)count;
	(void)dict_cache;
	return false;
#else
	if constexpr (!TRAITS::IS_SUPPORTED) {
		return false;
	} else {
		FORVector::ScanData<DOMAIN_T> left_scan;
		FORVector::ScanData<DOMAIN_T> right_scan;
		const bool left_for = FORVector::TryGetScanData(left, left_scan);
		const bool right_for = FORVector::TryGetScanData(right, right_scan);
		if (left_for == right_for) {
			return false;
		}
		auto &scan = left_for ? left_scan : right_scan;
		auto &constant_vector = left_for ? right : left;
		if (constant_vector.GetVectorType() != VectorType::CONSTANT_VECTOR || ConstantVector::IsNull(constant_vector)) {
			return false;
		}
		auto constant = ConstantVector::GetData<DOMAIN_T>(constant_vector)[0];
		if (NumericLimits<DOMAIN_T>::IsSigned() && constant < DOMAIN_T(0)) {
			return false;
		}

		DOMAIN_T result_min;
		DOMAIN_T result_max;
		const bool for_on_left = !TRAITS::IS_SUBTRACT || left_for;
		const auto lmin = for_on_left ? DOMAIN_T(0) : constant;
		const auto lmax = for_on_left ? scan.max_value : constant;
		const auto rmin = for_on_left ? constant : DOMAIN_T(0);
		const auto rmax = for_on_left ? constant : scan.max_value;
		if (!FORBounds<OP, DOMAIN_T>(lmin, lmax, rmin, rmax, result_min, result_max)) {
			return false;
		}
		if (result_min < DOMAIN_T(0)) {
			return false;
		}
		PhysicalType result_stored;
		if (!FORVector::TryGetStoredTypeForMax<DOMAIN_T>(result_max, result_stored)) {
			return false;
		}
		using EXECUTOR = FORStandardExecutor<typename TRAITS::ExecOp>;
		// the payload stays at its stored width; the constant carries the result width
		const auto compute = EXECUTOR::MaxStored(result_stored, scan.stored_type);
		auto fill_result = [&](Vector &target, idx_t target_count) {
			FORVector::Create<DOMAIN_T>(target, compute, result_max);
			// little-endian: reading the low bytes of the u64 storage truncates modularly (exact by bounds)
			uint64_t cval = static_cast<uint64_t>(constant);
			BinaryBufferArgs args;
			args.count = target_count;
			args.result_data = FORVector::GetData(target);
			args.result_validity = &FORVector::Validity(target);
			if (TRAITS::IS_SUBTRACT && !left_for) {
				args.ldata = const_data_ptr_cast(&cval);
				args.lconstant = true;
				args.rdata = FORVector::GetData(*scan.for_vec);
				args.rvalidity = scan.validity.get();
				EXECUTOR::Kernel(compute, scan.stored_type, compute)(args);
			} else {
				args.ldata = FORVector::GetData(*scan.for_vec);
				args.lvalidity = scan.validity.get();
				args.rdata = const_data_ptr_cast(&cval);
				args.rconstant = true;
				EXECUTOR::Kernel(scan.stored_type, compute, compute)(args);
			}
		};
		if (scan.sel) {
			const idx_t child_count = scan.for_vec->size();
			if (child_count > STANDARD_VECTOR_SIZE) {
				return false;
			}
			// reuse the cached dictionary child across chunks (mirrors TryExecuteDictionaryExpression)
			if (!dict_cache || dict_cache->data.GetType() != result.GetType()) {
				dict_cache = make_buffer<DictionaryEntry>(Vector(result.GetType(), STANDARD_VECTOR_SIZE));
				// full-stride allocation and pipeline-local, so consumers may widen it in place
				dict_cache->data.BufferMutable().cache_owned = true;
			}
			fill_result(dict_cache->data, child_count);
			result.Dictionary(dict_cache, *scan.sel, count);
		} else {
			fill_result(result, count);
		}
		return true;
	}
#endif
}

// FOR (op) FOR for op in {+, *}: compute the result width once, then run the registered kernel on both payloads.
template <class DOMAIN_T, class OP>
static bool TryFORColCol(Vector &left, Vector &right, Vector &result, idx_t count,
                         buffer_ptr<DictionaryEntry> &dict_cache, unique_ptr<Vector> &scratch) {
	using TRAITS = FOROpTraits<OP>;
#ifdef DUCKDB_SMALLER_BINARY
	(void)left;
	(void)right;
	(void)result;
	(void)count;
	(void)dict_cache;
	(void)scratch;
	return false;
#else
	if constexpr (!TRAITS::IS_SUPPORTED || TRAITS::IS_SUBTRACT) {
		return false;
	} else {
		FORVector::ScanData<DOMAIN_T> lscan;
		FORVector::ScanData<DOMAIN_T> rscan;
		if (!FORVector::TryGetScanData(left, lscan) || !FORVector::TryGetScanData(right, rscan)) {
			return false;
		}
		// both bare, or both sharing one dictionary selection
		if (lscan.sel || rscan.sel) {
			if (!lscan.sel || !rscan.sel || !SelectionVector::SameSelection(*lscan.sel, *rscan.sel)) {
				return false;
			}
		}
		const SelectionVector *sel = lscan.sel;
		if (count > STANDARD_VECTOR_SIZE) {
			return false;
		}
		DOMAIN_T result_max;
		if constexpr (TRAITS::IS_MULTIPLY) {
			if (!TryMultiplyOperator::Operation(lscan.max_value, rscan.max_value, result_max)) {
				return false;
			}
		} else {
			if (!TryAddOperator::Operation(lscan.max_value, rscan.max_value, result_max)) {
				return false;
			}
		}
		if (result_max < DOMAIN_T(0)) {
			return false;
		}
		PhysicalType res_stored;
		if (!FORVector::TryGetStoredTypeForMax<DOMAIN_T>(result_max, res_stored)) {
			return false;
		}
		using EXECUTOR = FORStandardExecutor<typename TRAITS::ExecOp>;
		const auto pair_max = EXECUTOR::MaxStored(lscan.stored_type, rscan.stored_type);
		const auto compute = EXECUTOR::MaxStored(res_stored, pair_max);
		const idx_t child_count = sel ? lscan.for_vec->size() : count;
		if (child_count > STANDARD_VECTOR_SIZE || (sel && rscan.for_vec->size() != child_count)) {
			return false;
		}
		auto fill_result = [&](Vector &target, idx_t n, const SelectionVector *gather_sel) {
			FORVector::Create<DOMAIN_T>(target, compute, result_max);
			auto ldata = FORVector::GetData(*lscan.for_vec);
			auto rdata = FORVector::GetData(*rscan.for_vec);
			auto lw = lscan.stored_type;
			auto rw = rscan.stored_type;
			if (compute != pair_max) {
				// the result outgrows both operands: align the wider side so the pair carries the result width
				const idx_t align_count = gather_sel ? child_count : n;
				if (GetTypeIdSize(lw) >= GetTypeIdSize(rw)) {
					ldata = EXECUTOR::Operand(ldata, lw, compute, align_count, scratch);
					lw = compute;
				} else {
					rdata = EXECUTOR::Operand(rdata, rw, compute, align_count, scratch);
					rw = compute;
				}
			}
			BinaryBufferArgs args;
			args.ldata = ldata;
			args.rdata = rdata;
			args.result_data = FORVector::GetData(target);
			args.count = n;
			args.lsel = gather_sel;
			args.rsel = gather_sel;
			args.lvalidity = lscan.validity.get();
			args.rvalidity = rscan.validity.get();
			args.result_validity = &FORVector::Validity(target);
			EXECUTOR::Kernel(lw, rw, compute)(args);
		};
		if (sel && !DenseAutoVecPaysOff(count, child_count, GetTypeIdSize(compute))) {
			// selective: gather both payloads through sel, result stays dense
			fill_result(result, count, sel);
		} else if (sel) {
			// dense enough: compute over the children once, wrap the result in the cached dictionary
			if (!dict_cache || dict_cache->data.GetType() != result.GetType()) {
				dict_cache = make_buffer<DictionaryEntry>(Vector(result.GetType(), STANDARD_VECTOR_SIZE));
				// full-stride allocation and pipeline-local, so consumers may widen it in place
				dict_cache->data.BufferMutable().cache_owned = true;
			}
			fill_result(dict_cache->data, child_count, nullptr);
			result.Dictionary(dict_cache, *sel, count);
		} else {
			fill_result(result, count, nullptr);
		}
		return true;
	}
#endif
}

} // namespace duckdb
