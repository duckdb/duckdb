//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"

namespace duckdb {

//! FOR-native column-vs-column comparison selection.
//!
//! When both operands of a comparison are FOR vectors (bare or dict-over-FOR), the comparison runs
//! directly on the narrow stored payloads, widening each value in-register, instead of decompressing
//! both sides to full-width flat vectors first. The inner loop mirrors BinaryExecutor::SelectGenericLoop
//! exactly (same indexing and true_sel/false_sel routing) so the result is bit-identical to the generic
//! path; only the data read changes (narrow + widen instead of a full-width flat read).

//! Inner loop: exactly BinaryExecutor::SelectGenericLoop, reading narrow payloads (LS/RS) and widening
//! to LOGICAL_T per element. The NO_NULL path carries no validity machinery.
template <class LOGICAL_T, class LS, class RS, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t ForSelectLoop(const LS *__restrict ldata, const RS *__restrict rdata,
                                  const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                  const SelectionVector &result_sel, idx_t count, const ValidityMask &lvalidity,
                                  const ValidityMask &rvalidity, SelectionVector *true_sel,
                                  SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = result_sel.get_index(i);
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		const LOGICAL_T lv = FORVector::WidenStored<LOGICAL_T>(ldata[lindex]);
		const LOGICAL_T rv = FORVector::WidenStored<LOGICAL_T>(rdata[rindex]);
		const bool comparison_result =
		    (NO_NULL || (lvalidity.RowIsValid(lindex) && rvalidity.RowIsValid(rindex))) && OP::Operation(lv, rv);
		if (HAS_TRUE_SEL) {
			true_sel->set_index(true_count, result_idx);
			true_count += comparison_result;
		}
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count, result_idx);
			false_count += !comparison_result;
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <class LOGICAL_T, class LS, class RS, class OP>
static inline idx_t ForSelectDispatch(const LS *ldata, const RS *rdata, const SelectionVector *lsel,
                                      const SelectionVector *rsel, const SelectionVector &result_sel, idx_t count,
                                      const ValidityMask &lvalidity, const ValidityMask &rvalidity, bool no_null,
                                      SelectionVector *true_sel, SelectionVector *false_sel) {
	if (no_null) {
		if (true_sel && false_sel) {
			return ForSelectLoop<LOGICAL_T, LS, RS, OP, true, true, true>(ldata, rdata, lsel, rsel, result_sel, count,
			                                                              lvalidity, rvalidity, true_sel, false_sel);
		} else if (true_sel) {
			return ForSelectLoop<LOGICAL_T, LS, RS, OP, true, true, false>(ldata, rdata, lsel, rsel, result_sel, count,
			                                                               lvalidity, rvalidity, true_sel, false_sel);
		} else {
			return ForSelectLoop<LOGICAL_T, LS, RS, OP, true, false, true>(ldata, rdata, lsel, rsel, result_sel, count,
			                                                               lvalidity, rvalidity, true_sel, false_sel);
		}
	} else {
		if (true_sel && false_sel) {
			return ForSelectLoop<LOGICAL_T, LS, RS, OP, false, true, true>(ldata, rdata, lsel, rsel, result_sel, count,
			                                                               lvalidity, rvalidity, true_sel, false_sel);
		} else if (true_sel) {
			return ForSelectLoop<LOGICAL_T, LS, RS, OP, false, true, false>(ldata, rdata, lsel, rsel, result_sel, count,
			                                                                lvalidity, rvalidity, true_sel, false_sel);
		} else {
			return ForSelectLoop<LOGICAL_T, LS, RS, OP, false, false, true>(ldata, rdata, lsel, rsel, result_sel, count,
			                                                                lvalidity, rvalidity, true_sel, false_sel);
		}
	}
}

template <class LOGICAL_T, class OP>
static bool ForSelectTyped(const Vector &left, const Vector &right, const SelectionVector *sel, idx_t count,
                           SelectionVector *true_sel, SelectionVector *false_sel, idx_t &result) {
	FORVector::ScanData<LOGICAL_T> lscan, rscan;
	if (!FORVector::TryGetScanData<LOGICAL_T>(left, lscan) || !FORVector::TryGetScanData<LOGICAL_T>(right, rscan)) {
		return false;
	}
	auto &result_sel = sel ? *sel : *FlatVector::IncrementalSelectionVector();
	auto lsel = lscan.sel ? lscan.sel : FlatVector::IncrementalSelectionVector();
	auto rsel = rscan.sel ? rscan.sel : FlatVector::IncrementalSelectionVector();
	const bool no_null = !lscan.validity->CanHaveNull() && !rscan.validity->CanHaveNull();
	FOR_SWITCH_STORED(lscan.stored_type, LS, {
		FOR_SWITCH_STORED(rscan.stored_type, RS, {
			auto ldata = reinterpret_cast<const LS *>(lscan.data);
			auto rdata = reinterpret_cast<const RS *>(rscan.data);
			result =
			    ForSelectDispatch<LOGICAL_T, LS, RS, OP>(ldata, rdata, lsel, rsel, result_sel, count, *lscan.validity,
			                                             *rscan.validity, no_null, true_sel, false_sel);
		});
	});
	return true;
}

//! Attempt FOR-native selection for `left <OP> right`. Returns false (nothing written) unless both
//! operands resolve as FOR vectors of a supported logical type, so the caller falls through to generic.
template <class OP>
static bool TryForSelectComparison(const Vector &left, const Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel, idx_t &result) {
	const SelectionVector *ldummy, *rdummy;
	if (!FORVector::TryGetFOR(left, ldummy) || !FORVector::TryGetFOR(right, rdummy)) {
		return false;
	}
	// a bound comparison has both sides at the same logical type
	switch (left.GetType().InternalType()) {
	case PhysicalType::INT16:
		return ForSelectTyped<int16_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::INT32:
		return ForSelectTyped<int32_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::INT64:
		return ForSelectTyped<int64_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::INT128:
		return ForSelectTyped<hugeint_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::UINT16:
		return ForSelectTyped<uint16_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::UINT32:
		return ForSelectTyped<uint32_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::UINT64:
		return ForSelectTyped<uint64_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	case PhysicalType::UINT128:
		return ForSelectTyped<uhugeint_t, OP>(left, right, sel, count, true_sel, false_sel, result);
	default:
		return false;
	}
}

} // namespace duckdb
