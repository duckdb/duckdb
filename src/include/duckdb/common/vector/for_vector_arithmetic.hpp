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

// FOR (op) constant: compute result width once, then run one narrow payload loop.
template <class DOMAIN_T, class OP>
static bool TryFORConstant(Vector &left, Vector &right, Vector &result, idx_t count) {
	using TRAITS = FOROpTraits<OP>;
	if constexpr (!TRAITS::IS_SUPPORTED || std::is_same<DOMAIN_T, hugeint_t>::value ||
	              std::is_same<DOMAIN_T, uhugeint_t>::value) {
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
		auto fill_result = [&](Vector &target, idx_t target_count) {
			FORVector::Create<DOMAIN_T>(target, result_stored, result_max);
			if (scan.validity->CanHaveNull()) {
				FORVector::Validity(target).Initialize(*scan.validity);
			}
			auto source =
			    FORVector::CreatePayloadView(scan.stored_type, FORVector::GetData(*scan.for_vec), target_count);
			auto target_payload = FORVector::CreatePayloadView(result_stored, FORVector::GetData(target), target_count);
			FOR_SWITCH_STORED(scan.stored_type, ST, {
				FOR_SWITCH_STORED(result_stored, RST, {
					Vector constant_view(Value::CreateValue(UnsafeNumericCast<RST>(constant)), count_t(target_count));
					if constexpr (TRAITS::IS_ADD) {
						BinaryExecutor::ExecuteStandard<ST, RST, RST, AddOperator>(source, constant_view,
						                                                           target_payload, target_count);
					} else if constexpr (TRAITS::IS_MULTIPLY) {
						BinaryExecutor::ExecuteStandard<ST, RST, RST, MultiplyOperator>(source, constant_view,
						                                                                target_payload, target_count);
					} else if (left_for) {
						BinaryExecutor::ExecuteStandard<ST, RST, RST, SubtractOperator>(source, constant_view,
						                                                                target_payload, target_count);
					} else {
						BinaryExecutor::ExecuteStandard<RST, ST, RST, SubtractOperator>(constant_view, source,
						                                                                target_payload, target_count);
					}
				});
			});
		};
		if (scan.sel) {
			Vector child(result.GetType(), scan.for_vec->size());
			fill_result(child, scan.for_vec->size());
			auto entry = make_buffer<DictionaryEntry>(std::move(child));
			result.Dictionary(std::move(entry), *scan.sel, count);
		} else {
			fill_result(result, count);
		}
		return true;
	}
}

// One-pass widening binary kernel: read narrow operands, cast to result width, apply OP, write result.
template <class OP>
struct WideningBinaryExecutor {
	template <class L, class R, class RES, bool GATHER>
	static void Loop(const L *__restrict l, const R *__restrict r, RES *__restrict res, const SelectionVector *sel,
	                 idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			const idx_t idx = GATHER ? sel->get_index(i) : i;
			res[i] = OP::template Operation<RES, RES, RES>(static_cast<RES>(l[idx]), static_cast<RES>(r[idx]));
		}
	}
	static void Execute(const_data_ptr_t l, PhysicalType lw, const_data_ptr_t r, PhysicalType rw, data_ptr_t res,
	                    PhysicalType res_w, const SelectionVector *sel, idx_t count) {
		FOR_SWITCH_STORED(lw, L, {
			FOR_SWITCH_STORED(rw, R, {
				FOR_SWITCH_STORED(res_w, RES, {
					auto lp = reinterpret_cast<const L *>(l);
					auto rp = reinterpret_cast<const R *>(r);
					auto resp = reinterpret_cast<RES *>(res);
					if (sel) {
						Loop<L, R, RES, true>(lp, rp, resp, sel, count);
					} else {
						Loop<L, R, RES, false>(lp, rp, resp, sel, count);
					}
				});
			});
		});
	}
};

// FOR (op) FOR for op in {+, *}: compute result width once, then run one widening payload loop.
template <class DOMAIN_T, class OP>
static bool TryFORColCol(Vector &left, Vector &right, Vector &result, idx_t count) {
	using TRAITS = FOROpTraits<OP>;
	if constexpr (!TRAITS::IS_SUPPORTED || TRAITS::IS_SUBTRACT || std::is_same<DOMAIN_T, hugeint_t>::value ||
	              std::is_same<DOMAIN_T, uhugeint_t>::value) {
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
		FORVector::Create<DOMAIN_T>(result, res_stored, result_max);
		const bool lnull = lscan.validity->CanHaveNull();
		const bool rnull = rscan.validity->CanHaveNull();
		if (lnull || rnull) {
			auto &result_validity = FORVector::Validity(result);
			if (!sel) {
				if (lnull) {
					result_validity.Initialize(*lscan.validity);
					if (rnull) {
						result_validity.Combine(*rscan.validity, count);
					}
				} else {
					result_validity.Initialize(*rscan.validity);
				}
			} else {
				for (idx_t i = 0; i < count; i++) {
					const auto idx = sel->get_index(i);
					if ((lnull && !lscan.validity->RowIsValid(idx)) || (rnull && !rscan.validity->RowIsValid(idx))) {
						result_validity.SetInvalid(i);
					}
				}
			}
		}
		using EXEC_OP = typename TRAITS::ExecOp;
		WideningBinaryExecutor<EXEC_OP>::Execute(lscan.data, lscan.stored_type, rscan.data, rscan.stored_type,
		                                         FORVector::GetData(result), res_stored, sel, count);
		return true;
	}
}

} // namespace duckdb
