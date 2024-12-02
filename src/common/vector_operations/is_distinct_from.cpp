#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

struct DistinctBinaryLambdaWrapper {
	template <class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right, bool is_left_null, bool is_right_null) {
		return OP::template Operation<LEFT_TYPE>(left, right, is_left_null, is_right_null);
	}
};

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteGenericLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                                       RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
                                       const SelectionVector *__restrict rsel, idx_t count, ValidityMask &lmask,
                                       ValidityMask &rmask, ValidityMask &result_mask) {
	for (idx_t i = 0; i < count; i++) {
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		auto lentry = ldata[lindex];
		auto rentry = rdata[rindex];
		result_data[i] =
		    OP::template Operation<LEFT_TYPE>(lentry, rentry, !lmask.RowIsValid(lindex), !rmask.RowIsValid(rindex));
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteConstant(Vector &left, Vector &right, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);

	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
	auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
	*result_data =
	    OP::template Operation<LEFT_TYPE>(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right));
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteGeneric(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		DistinctExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result);
	} else {
		UnifiedVectorFormat ldata, rdata;

		left.ToUnifiedFormat(count, ldata);
		right.ToUnifiedFormat(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		DistinctExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(
		    UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata), UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata),
		    result_data, ldata.sel, rdata.sel, count, ldata.validity, rdata.validity, FlatVector::Validity(result));
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteSwitch(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, count);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, count);
}

#ifndef DUCKDB_SMALLER_BINARY
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
#else
template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
#endif
static inline idx_t
DistinctSelectGenericLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                          const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                          const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                          ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
#ifdef DUCKDB_SMALLER_BINARY
	bool HAS_TRUE_SEL = true_sel;
	bool HAS_FALSE_SEL = false_sel;
#endif
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = result_sel->get_index(i);
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
#ifndef DUCKDB_SMALLER_BINARY
		if (NO_NULL) {
			if (OP::Operation(ldata[lindex], rdata[rindex], false, false)) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		} else
#endif
		{
			if (OP::Operation(ldata[lindex], rdata[rindex], !lmask.RowIsValid(lindex), !rmask.RowIsValid(rindex))) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

#ifndef DUCKDB_SMALLER_BINARY
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL>
static inline idx_t
DistinctSelectGenericLoopSelSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                                   const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                   const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                                   ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	}
}
#endif

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static inline idx_t
DistinctSelectGenericLoopSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                                const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                                ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
#ifndef DUCKDB_SMALLER_BINARY
	if (!lmask.AllValid() || !rmask.AllValid()) {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	}
#else
	return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP>(ldata, rdata, lsel, rsel, result_sel, count, lmask,
	                                                            rmask, true_sel, false_sel);
#endif
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectGeneric(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	UnifiedVectorFormat ldata, rdata;

	left.ToUnifiedFormat(count, ldata);
	right.ToUnifiedFormat(count, rdata);

	return DistinctSelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>(
	    UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata), UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata), ldata.sel,
	    rdata.sel, sel, count, ldata.validity, rdata.validity, true_sel, false_sel);
}

#ifndef DUCKDB_SMALLER_BINARY
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL,
          bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DistinctSelectFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                           const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                           ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t result_idx = sel->get_index(i);
		idx_t lidx = LEFT_CONSTANT ? 0 : i;
		idx_t ridx = RIGHT_CONSTANT ? 0 : i;
		const bool lnull = !lmask.RowIsValid(lidx);
		const bool rnull = !rmask.RowIsValid(ridx);
		bool comparison_result = OP::Operation(ldata[lidx], rdata[ridx], lnull, rnull);
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

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL>
static inline idx_t DistinctSelectFlatLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                    const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                                    ValidityMask &rmask, SelectionVector *true_sel,
                                                    SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, true>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, false>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, false, true>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static inline idx_t DistinctSelectFlatLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                 const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                                 ValidityMask &rmask, SelectionVector *true_sel,
                                                 SelectionVector *false_sel) {
	return DistinctSelectFlatLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true>(
	    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static idx_t DistinctSelectFlat(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
	auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);
	if (LEFT_CONSTANT) {
		ValidityMask validity;
		if (ConstantVector::IsNull(left)) {
			validity.SetAllInvalid(1);
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, validity, FlatVector::Validity(right), true_sel, false_sel);
	} else if (RIGHT_CONSTANT) {
		ValidityMask validity;
		if (ConstantVector::IsNull(right)) {
			validity.SetAllInvalid(1);
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Validity(left), validity, true_sel, false_sel);
	} else {
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Validity(left), FlatVector::Validity(right), true_sel, false_sel);
	}
}
#endif

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectConstant(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);

	// both sides are constant, return either 0 or the count
	// in this case we do not fill in the result selection vector at all
	if (!OP::Operation(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right))) {
		if (false_sel) {
			for (idx_t i = 0; i < count; i++) {
				false_sel->set_index(i, sel->get_index(i));
			}
		}
		return 0;
	} else {
		if (true_sel) {
			for (idx_t i = 0; i < count; i++) {
				true_sel->set_index(i, sel->get_index(i));
			}
		}
		return count;
	}
}

static void UpdateNullMask(Vector &vec, const SelectionVector &sel, idx_t count, ValidityMask &null_mask) {
	UnifiedVectorFormat vdata;
	vec.ToUnifiedFormat(count, vdata);

	if (vdata.validity.AllValid()) {
		return;
	}

	for (idx_t i = 0; i < count; ++i) {
		const auto ridx = sel.get_index(i);
		const auto vidx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(vidx)) {
			null_mask.SetInvalid(ridx);
		}
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelect(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                            SelectionVector *true_sel, SelectionVector *false_sel,
                            optional_ptr<ValidityMask> null_mask) {
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}

	// TODO: Push this down?
	if (null_mask) {
		UpdateNullMask(left, *sel, count, *null_mask);
		UpdateNullMask(right, *sel, count, *null_mask);
	}

	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
#ifndef DUCKDB_SMALLER_BINARY
	} else if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	           right.GetVectorType() == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
	           right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::FLAT_VECTOR && right.GetVectorType() == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, sel, count, true_sel,
		                                                                   false_sel);
#endif
	} else {
		return DistinctSelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	}
}

template <class OP>
static idx_t DistinctSelectNotNull(Vector &left, Vector &right, const idx_t count, idx_t &true_count,
                                   const SelectionVector &sel, SelectionVector &maybe_vec, OptionalSelection &true_opt,
                                   OptionalSelection &false_opt, optional_ptr<ValidityMask> null_mask) {
	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(count, lvdata);
	right.ToUnifiedFormat(count, rvdata);

	auto &lmask = lvdata.validity;
	auto &rmask = rvdata.validity;

	idx_t remaining = 0;
	if (lmask.AllValid() && rmask.AllValid()) {
		//	None are NULL, distinguish values.
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel.get_index(i);
			maybe_vec.set_index(remaining++, idx);
		}
		return remaining;
	}

	// Slice the Vectors down to the rows that are not determined (i.e., neither is NULL)
	SelectionVector slicer(count);
	true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto result_idx = sel.get_index(i);
		const auto lidx = lvdata.sel->get_index(i);
		const auto ridx = rvdata.sel->get_index(i);
		const auto lnull = !lmask.RowIsValid(lidx);
		const auto rnull = !rmask.RowIsValid(ridx);
		if (lnull || rnull) {
			// If either is NULL then we can major distinguish them
			if (null_mask) {
				null_mask->SetInvalid(result_idx);
			}
			if (!OP::Operation(false, false, lnull, rnull)) {
				false_opt.Append(false_count, result_idx);
			} else {
				true_opt.Append(true_count, result_idx);
			}
		} else {
			//	Neither is NULL, distinguish values.
			slicer.set_index(remaining, i);
			maybe_vec.set_index(remaining++, result_idx);
		}
	}

	true_opt.Advance(true_count);
	false_opt.Advance(false_count);

	if (remaining && remaining < count) {
		left.Slice(slicer, remaining);
		right.Slice(slicer, remaining);
	}

	return remaining;
}

struct PositionComparator {
	// Select the rows that definitely match.
	// Default to the same as the final row
	template <typename OP>
	static idx_t Definite(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                      optional_ptr<SelectionVector> true_sel, SelectionVector &false_sel,
	                      optional_ptr<ValidityMask> null_mask) {
		return Final<OP>(left, right, sel, count, true_sel, &false_sel, null_mask);
	}

	// Select the possible rows that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static idx_t Possible(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                      SelectionVector &true_sel, optional_ptr<SelectionVector> false_sel,
	                      optional_ptr<ValidityMask> null_mask) {
		return VectorOperations::NestedEquals(left, right, &sel, count, &true_sel, false_sel, null_mask);
	}

	// Select the matching rows for the final position.
	// This needs to be specialised.
	template <typename OP>
	static idx_t Final(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                   optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                   optional_ptr<ValidityMask> null_mask) {
		return 0;
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos, false, false);
	}
};

// NotDistinctFrom must always check every column
template <>
idx_t PositionComparator::Definite<duckdb::NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                            idx_t count, optional_ptr<SelectionVector> true_sel,
                                                            SelectionVector &false_sel,
                                                            optional_ptr<ValidityMask> null_mask) {
	return 0;
}

template <>
idx_t PositionComparator::Final<duckdb::NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                         idx_t count, optional_ptr<SelectionVector> true_sel,
                                                         optional_ptr<SelectionVector> false_sel,
                                                         optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::NestedEquals(left, right, &sel, count, true_sel, false_sel, null_mask);
}

// DistinctFrom must check everything that matched
template <>
idx_t PositionComparator::Possible<duckdb::DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                         idx_t count, SelectionVector &true_sel,
                                                         optional_ptr<SelectionVector> false_sel,
                                                         optional_ptr<ValidityMask> null_mask) {
	return count;
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                      idx_t count, optional_ptr<SelectionVector> true_sel,
                                                      optional_ptr<SelectionVector> false_sel,
                                                      optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::NestedNotEquals(left, right, &sel, count, true_sel, false_sel, null_mask);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
idx_t PositionComparator::Definite<duckdb::DistinctLessThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector &sel, idx_t count,
                                                                   optional_ptr<SelectionVector> true_sel,
                                                                   SelectionVector &false_sel,
                                                                   optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThan(right, left, &sel, count, true_sel, &false_sel, null_mask);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                                idx_t count, optional_ptr<SelectionVector> true_sel,
                                                                optional_ptr<SelectionVector> false_sel,
                                                                optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThanEquals(right, left, &sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t PositionComparator::Definite<duckdb::DistinctGreaterThanEquals>(Vector &left, Vector &right,
                                                                      const SelectionVector &sel, idx_t count,
                                                                      optional_ptr<SelectionVector> true_sel,
                                                                      SelectionVector &false_sel,
                                                                      optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, &false_sel, null_mask);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector &sel, idx_t count,
                                                                   optional_ptr<SelectionVector> true_sel,
                                                                   optional_ptr<SelectionVector> false_sel,
                                                                   optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel, null_mask);
}

// Strict inequalities just use strict for both Definite and Final
template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThan>(Vector &left, Vector &right, const SelectionVector &sel,
                                                          idx_t count, optional_ptr<SelectionVector> true_sel,
                                                          optional_ptr<SelectionVector> false_sel,
                                                          optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThan(right, left, &sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThanNullsFirst>(Vector &left, Vector &right,
                                                                    const SelectionVector &sel, idx_t count,
                                                                    optional_ptr<SelectionVector> true_sel,
                                                                    optional_ptr<SelectionVector> false_sel,
                                                                    optional_ptr<ValidityMask> null_mask) {
	// DistinctGreaterThan has NULLs last
	return VectorOperations::DistinctGreaterThan(right, left, &sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThan>(Vector &left, Vector &right, const SelectionVector &sel,
                                                             idx_t count, optional_ptr<SelectionVector> true_sel,
                                                             optional_ptr<SelectionVector> false_sel,
                                                             optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThanNullsFirst>(Vector &left, Vector &right,
                                                                       const SelectionVector &sel, idx_t count,
                                                                       optional_ptr<SelectionVector> true_sel,
                                                                       optional_ptr<SelectionVector> false_sel,
                                                                       optional_ptr<ValidityMask> null_mask) {
	// DistinctLessThan has NULLs last
	return VectorOperations::DistinctLessThan(right, left, &sel, count, true_sel, false_sel, null_mask);
}

using StructEntries = vector<unique_ptr<Vector>>;

static void ExtractNestedSelection(const SelectionVector &slice_sel, const idx_t count, const SelectionVector &sel,
                                   OptionalSelection &opt) {

	for (idx_t i = 0; i < count;) {
		const auto slice_idx = slice_sel.get_index(i);
		const auto result_idx = sel.get_index(slice_idx);
		opt.Append(i, result_idx);
	}
	opt.Advance(count);
}

static void ExtractNestedMask(const SelectionVector &slice_sel, const idx_t count, const SelectionVector &sel,
                              ValidityMask *child_mask, optional_ptr<ValidityMask> null_mask) {

	if (!child_mask) {
		return;
	}

	for (idx_t i = 0; i < count; ++i) {
		const auto slice_idx = slice_sel.get_index(i);
		const auto result_idx = sel.get_index(slice_idx);
		if (child_mask && !child_mask->RowIsValid(slice_idx)) {
			null_mask->SetInvalid(result_idx);
		}
	}

	child_mask->Reset(null_mask->Capacity());
}

static void DensifyNestedSelection(const SelectionVector &dense_sel, const idx_t count, SelectionVector &slice_sel) {
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, dense_sel.get_index(i));
	}
}

template <class OP>
static idx_t DistinctSelectStruct(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                  OptionalSelection &true_opt, OptionalSelection &false_opt,
                                  optional_ptr<ValidityMask> null_mask) {
	if (count == 0) {
		return 0;
	}

	// Avoid allocating in the 99% of the cases where we don't need to.
	StructEntries lsliced, rsliced;
	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	const auto vcount = count;
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	ValidityMask child_validity;
	ValidityMask *child_mask = nullptr;
	if (null_mask) {
		child_mask = &child_validity;
		child_mask->Reset(null_mask->Capacity());
	}

	idx_t match_count = 0;
	for (idx_t col_no = 0; col_no < lchildren.size(); ++col_no) {
		// Slice the children to maintain density
		Vector lchild(*lchildren[col_no]);
		lchild.Flatten(vcount);
		lchild.Slice(slice_sel, count);

		Vector rchild(*rchildren[col_no]);
		rchild.Flatten(vcount);
		rchild.Slice(slice_sel, count);

		// Find everything that definitely matches
		auto true_count =
		    PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel, child_mask);
		// Extract any NULLs we found
		ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);
		if (true_count > 0) {
			auto false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);

			// Remove the definite matches from the slicing vector
			DensifyNestedSelection(false_sel, false_count, slice_sel);

			match_count += true_count;
			count -= true_count;
		}

		if (col_no != lchildren.size() - 1) {
			// Find what might match on the next position
			true_count =
			    PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel, child_mask);
			auto false_count = count - true_count;

			// Extract any NULLs we found
			ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);

			// Extract the definite failures into the false result
			ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

			// Remove any definite failures from the slicing vector
			if (false_count) {
				DensifyNestedSelection(true_sel, true_count, slice_sel);
			}

			count = true_count;
		} else {
			true_count =
			    PositionComparator::Final<OP>(lchild, rchild, slice_sel, count, &true_sel, &false_sel, child_mask);
			auto false_count = count - true_count;

			// Extract any NULLs we found
			ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);

			// Extract the definite failures into the false result
			ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

			match_count += true_count;
		}
	}
	return match_count;
}

static void PositionListCursor(SelectionVector &cursor, UnifiedVectorFormat &vdata, const idx_t pos,
                               const SelectionVector &slice_sel, const idx_t count) {
	const auto data = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
	for (idx_t i = 0; i < count; ++i) {
		const auto slice_idx = slice_sel.get_index(i);

		const auto lidx = vdata.sel->get_index(slice_idx);
		const auto &entry = data[lidx];
		cursor.set_index(i, entry.offset + pos);
	}
}

template <class OP>
static idx_t DistinctSelectList(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                OptionalSelection &true_opt, OptionalSelection &false_opt,
                                optional_ptr<ValidityMask> null_mask) {
	if (count == 0) {
		return count;
	}

	// Create dictionary views of the children so we can vectorise the positional comparisons.
	SelectionVector lcursor(count);
	SelectionVector rcursor(count);

	Vector lentry_flattened(ListVector::GetEntry(left));
	Vector rentry_flattened(ListVector::GetEntry(right));
	lentry_flattened.Flatten(ListVector::GetListSize(left));
	rentry_flattened.Flatten(ListVector::GetListSize(right));
	Vector lchild(lentry_flattened, lcursor, count);
	Vector rchild(rentry_flattened, rcursor, count);

	// To perform the positional comparison, we use a vectorisation of the following algorithm:
	// bool CompareLists(T *left, idx_t nleft, T *right, nright) {
	// 	for (idx_t pos = 0; ; ++pos) {
	// 		if (nleft == pos || nright == pos)
	// 			return OP::TieBreak(nleft, nright);
	// 		if (OP::Definite(*left, *right))
	// 			return true;
	// 		if (!OP::Maybe(*left, *right))
	// 			return false;
	// 		}
	//	 	++left, ++right;
	// 	}
	// }

	// Get pointers to the list entries
	UnifiedVectorFormat lvdata;
	left.ToUnifiedFormat(count, lvdata);
	const auto ldata = UnifiedVectorFormat::GetData<list_entry_t>(lvdata);

	UnifiedVectorFormat rvdata;
	right.ToUnifiedFormat(count, rvdata);
	const auto rdata = UnifiedVectorFormat::GetData<list_entry_t>(rvdata);

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	ValidityMask child_validity;
	ValidityMask *child_mask = nullptr;
	if (null_mask) {
		child_mask = &child_validity;
		child_mask->Reset(null_mask->Capacity());
	}

	idx_t match_count = 0;
	for (idx_t pos = 0; count > 0; ++pos) {
		// Set up the cursors for the current position
		PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
		PositionListCursor(rcursor, rvdata, pos, slice_sel, count);

		// Tie-break the pairs where one of the LISTs is exhausted.
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t maybe_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto slice_idx = slice_sel.get_index(i);
			const auto lidx = lvdata.sel->get_index(slice_idx);
			const auto &lentry = ldata[lidx];
			const auto ridx = rvdata.sel->get_index(slice_idx);
			const auto &rentry = rdata[ridx];
			if (lentry.length == pos || rentry.length == pos) {
				const auto idx = sel.get_index(slice_idx);
				if (PositionComparator::TieBreak<OP>(lentry.length, rentry.length)) {
					true_opt.Append(true_count, idx);
				} else {
					false_opt.Append(false_count, idx);
				}
			} else {
				true_sel.set_index(maybe_count++, slice_idx);
			}
		}
		true_opt.Advance(true_count);
		false_opt.Advance(false_count);
		match_count += true_count;

		// Redensify the list cursors
		if (maybe_count < count) {
			count = maybe_count;
			DensifyNestedSelection(true_sel, count, slice_sel);
			PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
			PositionListCursor(rcursor, rvdata, pos, slice_sel, count);
		}

		// Find everything that definitely matches
		true_count =
		    PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel, child_mask);
		// Extract any NULLs we found
		ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);
		if (true_count) {
			false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);
			match_count += true_count;

			// Redensify the list cursors
			count -= true_count;
			DensifyNestedSelection(false_sel, count, slice_sel);
			PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
			PositionListCursor(rcursor, rvdata, pos, slice_sel, count);
		}

		// Find what might match on the next position
		true_count =
		    PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel, child_mask);
		false_count = count - true_count;

		// Extract any NULLs we found
		ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);

		// Extract the definite failures into the false result
		ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

		if (false_count) {
			DensifyNestedSelection(true_sel, true_count, slice_sel);
		}
		count = true_count;
	}

	return match_count;
}

static void PositionArrayCursor(SelectionVector &cursor, UnifiedVectorFormat &vdata, const idx_t pos,
                                const SelectionVector &slice_sel, const idx_t count, idx_t array_size) {
	for (idx_t i = 0; i < count; ++i) {
		const auto slice_idx = slice_sel.get_index(i);
		const auto lidx = vdata.sel->get_index(slice_idx);
		const auto offset = array_size * lidx;
		cursor.set_index(i, offset + pos);
	}
}

template <class OP>
static idx_t DistinctSelectArray(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                 OptionalSelection &true_opt, OptionalSelection &false_opt,
                                 optional_ptr<ValidityMask> null_mask) {
	if (count == 0) {
		return count;
	}

	// FIXME: This function can probably be optimized since we know the array size is fixed for every entry.

	D_ASSERT(ArrayType::GetSize(left.GetType()) == ArrayType::GetSize(right.GetType()));
	auto array_size = ArrayType::GetSize(left.GetType());

	// Create dictionary views of the children so we can vectorise the positional comparisons.
	SelectionVector lcursor(count);
	SelectionVector rcursor(count);

	Vector lentry_flattened(ArrayVector::GetEntry(left));
	Vector rentry_flattened(ArrayVector::GetEntry(right));
	lentry_flattened.Flatten(ArrayVector::GetTotalSize(left));
	rentry_flattened.Flatten(ArrayVector::GetTotalSize(right));
	Vector lchild(lentry_flattened, lcursor, count);
	Vector rchild(rentry_flattened, rcursor, count);

	// Get pointers to the list entries
	UnifiedVectorFormat lvdata;
	left.ToUnifiedFormat(count, lvdata);

	UnifiedVectorFormat rvdata;
	right.ToUnifiedFormat(count, rvdata);

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	ValidityMask child_validity;
	ValidityMask *child_mask = nullptr;
	if (null_mask) {
		child_mask = &child_validity;
		child_mask->Reset(null_mask->Capacity());
	}

	idx_t match_count = 0;
	for (idx_t pos = 0; count > 0; ++pos) {
		// Set up the cursors for the current position
		PositionArrayCursor(lcursor, lvdata, pos, slice_sel, count, array_size);
		PositionArrayCursor(rcursor, rvdata, pos, slice_sel, count, array_size);

		// Tie-break the pairs where one of the LISTs is exhausted.
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t maybe_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto slice_idx = slice_sel.get_index(i);
			if (array_size == pos) {
				const auto idx = sel.get_index(slice_idx);
				if (PositionComparator::TieBreak<OP>(array_size, array_size)) {
					true_opt.Append(true_count, idx);
				} else {
					false_opt.Append(false_count, idx);
				}
			} else {
				true_sel.set_index(maybe_count++, slice_idx);
			}
		}
		true_opt.Advance(true_count);
		false_opt.Advance(false_count);
		match_count += true_count;

		// Redensify the list cursors
		if (maybe_count < count) {
			count = maybe_count;
			DensifyNestedSelection(true_sel, count, slice_sel);
			PositionArrayCursor(lcursor, lvdata, pos, slice_sel, count, array_size);
			PositionArrayCursor(rcursor, rvdata, pos, slice_sel, count, array_size);
		}

		// Find everything that definitely matches
		true_count =
		    PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel, child_mask);
		// Extract any NULLs we found
		ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);
		if (true_count) {
			false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);
			match_count += true_count;

			// Redensify the list cursors
			count -= true_count;
			DensifyNestedSelection(false_sel, count, slice_sel);
			PositionArrayCursor(lcursor, lvdata, pos, slice_sel, count, array_size);
			PositionArrayCursor(rcursor, rvdata, pos, slice_sel, count, array_size);
		}

		// Find what might match on the next position
		true_count =
		    PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel, null_mask);
		false_count = count - true_count;

		// Extract any NULLs we found
		ExtractNestedMask(slice_sel, count, sel, child_mask, null_mask);

		// Extract the definite failures into the false result
		ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

		if (false_count) {
			DensifyNestedSelection(true_sel, true_count, slice_sel);
		}
		count = true_count;
	}

	return match_count;
}

template <class OP>
static idx_t DistinctSelectNested(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                  const idx_t count, optional_ptr<SelectionVector> true_sel,
                                  optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	// The Select operations all use a dense pair of input vectors to partition
	// a selection vector in a single pass. But to implement progressive comparisons,
	// we have to make multiple passes, so we need to keep track of the original input positions
	// and then scatter the output selections when we are done.
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}

	// Make buffered selections for progressive comparisons
	// TODO: Remove unnecessary allocations
	SelectionVector true_vec(count);
	OptionalSelection true_opt(&true_vec);

	SelectionVector false_vec(count);
	OptionalSelection false_opt(&false_vec);

	SelectionVector maybe_vec(count);

	// Handle NULL nested values
	Vector l_not_null(left);
	Vector r_not_null(right);

	idx_t match_count = 0;
	auto unknown = DistinctSelectNotNull<OP>(l_not_null, r_not_null, count, match_count, *sel, maybe_vec, true_opt,
	                                         false_opt, null_mask);

	switch (left.GetType().InternalType()) {
	case PhysicalType::LIST:
		match_count +=
		    DistinctSelectList<OP>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt, null_mask);
		break;
	case PhysicalType::STRUCT:
		match_count +=
		    DistinctSelectStruct<OP>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt, null_mask);
		break;
	case PhysicalType::ARRAY:
		match_count +=
		    DistinctSelectArray<OP>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt, null_mask);
		break;
	default:
		throw NotImplementedException("Unimplemented type for DISTINCT");
	}

	// Copy the buffered selections to the output selections
	if (true_sel) {
		DensifyNestedSelection(true_vec, match_count, *true_sel);
	}

	if (false_sel) {
		DensifyNestedSelection(false_vec, count - match_count, *false_sel);
	}

	return match_count;
}

template <typename OP>
static void NestedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count);

template <class T, class OP>
static inline void TemplatedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecute<T, T, bool, OP>(left, right, result, count);
}
template <class OP>
static void ExecuteDistinct(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType() == right.GetType() && result.GetType() == LogicalType::BOOLEAN);
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedDistinctExecute<int8_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedDistinctExecute<int16_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedDistinctExecute<int32_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedDistinctExecute<int64_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedDistinctExecute<uint8_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedDistinctExecute<uint16_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedDistinctExecute<uint32_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedDistinctExecute<uint64_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedDistinctExecute<hugeint_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT128:
		TemplatedDistinctExecute<uhugeint_t, OP>(left, right, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedDistinctExecute<float, OP>(left, right, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDistinctExecute<double, OP>(left, right, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDistinctExecute<interval_t, OP>(left, right, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedDistinctExecute<string_t, OP>(left, right, result, count);
		break;
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
	case PhysicalType::ARRAY:
		NestedDistinctExecute<OP>(left, right, result, count);
		break;
	default:
		throw InternalException("Invalid type for distinct comparison");
	}
}

template <class OP>
static idx_t TemplatedDistinctSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                              idx_t count, optional_ptr<SelectionVector> true_sel,
                                              optional_ptr<SelectionVector> false_sel,
                                              optional_ptr<ValidityMask> null_mask) {

	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return DistinctSelect<int8_t, int8_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                          null_mask);
	case PhysicalType::INT16:
		return DistinctSelect<int16_t, int16_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                            null_mask);
	case PhysicalType::INT32:
		return DistinctSelect<int32_t, int32_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                            null_mask);
	case PhysicalType::INT64:
		return DistinctSelect<int64_t, int64_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                            null_mask);
	case PhysicalType::UINT8:
		return DistinctSelect<uint8_t, uint8_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                            null_mask);
	case PhysicalType::UINT16:
		return DistinctSelect<uint16_t, uint16_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                              null_mask);
	case PhysicalType::UINT32:
		return DistinctSelect<uint32_t, uint32_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                              null_mask);
	case PhysicalType::UINT64:
		return DistinctSelect<uint64_t, uint64_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                              null_mask);
	case PhysicalType::INT128:
		return DistinctSelect<hugeint_t, hugeint_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                                null_mask);
	case PhysicalType::UINT128:
		return DistinctSelect<uhugeint_t, uhugeint_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                  false_sel.get(), null_mask);
	case PhysicalType::FLOAT:
		return DistinctSelect<float, float, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                        null_mask);
	case PhysicalType::DOUBLE:
		return DistinctSelect<double, double, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                          null_mask);
	case PhysicalType::INTERVAL:
		return DistinctSelect<interval_t, interval_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                  false_sel.get(), null_mask);
	case PhysicalType::VARCHAR:
		return DistinctSelect<string_t, string_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get(),
		                                              null_mask);
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return DistinctSelectNested<OP>(left, right, sel, count, true_sel, false_sel, null_mask);
	default:
		throw InternalException("Invalid type for distinct selection");
	}
}

template <typename OP>
static void NestedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if (left_constant && right_constant) {
		// both sides are constant, so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		SelectionVector true_sel(1);
		auto match_count = TemplatedDistinctSelectOperation<OP>(left, right, nullptr, 1, &true_sel, nullptr, nullptr);
		result_data[0] = match_count > 0;
		return;
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	// DISTINCT is either true or false
	idx_t match_count =
	    TemplatedDistinctSelectOperation<OP>(left, right, nullptr, count, &true_sel, &false_sel, nullptr);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
	}
}

void VectorOperations::DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::DistinctFrom>(left, right, result, count);
}

void VectorOperations::NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::NotDistinctFrom>(left, right, result, count);
}

// true := A != B with nulls being equal
idx_t VectorOperations::DistinctFrom(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                     optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, true_sel, false_sel,
	                                                              nullptr);
}
// true := A == B with nulls being equal
idx_t VectorOperations::NotDistinctFrom(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                        idx_t count, optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel) {
	return count - TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, false_sel, true_sel,
	                                                                      nullptr);
}

// true := A > B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                            idx_t count, optional_ptr<SelectionVector> true_sel,
                                            optional_ptr<SelectionVector> false_sel,
                                            optional_ptr<ValidityMask> null_mask) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(left, right, sel, count, true_sel, false_sel,
	                                                                     null_mask);
}

// true := A > B with nulls being minimal
idx_t VectorOperations::DistinctGreaterThanNullsFirst(Vector &left, Vector &right,
                                                      optional_ptr<const SelectionVector> sel, idx_t count,
                                                      optional_ptr<SelectionVector> true_sel,
                                                      optional_ptr<SelectionVector> false_sel,
                                                      optional_ptr<ValidityMask> null_mask) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThanNullsFirst>(left, right, sel, count, true_sel,
	                                                                               false_sel, null_mask);
}

// true := A >= B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                                  idx_t count, optional_ptr<SelectionVector> true_sel,
                                                  optional_ptr<SelectionVector> false_sel,
                                                  optional_ptr<ValidityMask> null_mask) {
	return count - TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(right, left, sel, count, false_sel,
	                                                                             true_sel, null_mask);
}
// true := A < B with nulls being maximal
idx_t VectorOperations::DistinctLessThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                         idx_t count, optional_ptr<SelectionVector> true_sel,
                                         optional_ptr<SelectionVector> false_sel,
                                         optional_ptr<ValidityMask> null_mask) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(right, left, sel, count, true_sel, false_sel,
	                                                                     null_mask);
}

// true := A < B with nulls being minimal
idx_t VectorOperations::DistinctLessThanNullsFirst(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                                   idx_t count, optional_ptr<SelectionVector> true_sel,
                                                   optional_ptr<SelectionVector> false_sel,
                                                   optional_ptr<ValidityMask> null_mask) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThanNullsFirst>(right, left, sel, count, true_sel,
	                                                                               false_sel, nullptr);
}

// true := A <= B with nulls being maximal
idx_t VectorOperations::DistinctLessThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                               idx_t count, optional_ptr<SelectionVector> true_sel,
                                               optional_ptr<SelectionVector> false_sel,
                                               optional_ptr<ValidityMask> null_mask) {
	return count - TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(left, right, sel, count, false_sel,
	                                                                             true_sel, null_mask);
}

// true := A != B with nulls being equal, inputs selected
idx_t VectorOperations::NestedNotEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                        idx_t count, optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, true_sel, false_sel,
	                                                              null_mask);
}
// true := A == B with nulls being equal, inputs selected
idx_t VectorOperations::NestedEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                     optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                     optional_ptr<ValidityMask> null_mask) {
	return count - TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, false_sel, true_sel,
	                                                                      null_mask);
}

} // namespace duckdb
