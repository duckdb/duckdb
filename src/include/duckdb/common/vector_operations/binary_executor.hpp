//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>

namespace duckdb {

struct DefaultNullCheckOperator {
	template <class LEFT_TYPE, class RIGHT_TYPE>
	static inline bool Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return false;
	}
};

struct BinaryStandardOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct BinarySingleArgumentOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE>(left, right);
	}

	static bool AddsNulls() {
		return false;
	}
};

template <bool ADDS_NULLS>
struct BinaryLambdaWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		if constexpr (ADDS_NULLS) {
			auto result = fun(left, right);
			if (!result.has_value()) {
				mask.SetInvalid(idx);
				return RESULT_TYPE();
			}
			return result.value();
		} else {
			return fun(left, right);
		}
	}

	static bool AddsNulls() {
		return ADDS_NULLS;
	}
};

struct BinaryExecutor {
#ifndef DUCKDB_SMALLER_BINARY
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static void ExecuteFlatLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                            RESULT_TYPE *__restrict result_data, idx_t count, ValidityMask &mask, FUNC fun) {
		if (!LEFT_CONSTANT) {
			ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
		}
		if (!RIGHT_CONSTANT) {
			ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
		}

		if (mask.CanHaveNull()) {
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						auto lentry = ldata[LEFT_CONSTANT ? 0 : base_idx];
						auto rentry = rdata[RIGHT_CONSTANT ? 0 : base_idx];
						result_data[base_idx] =
						    OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
						        fun, lentry, rentry, mask, base_idx);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							auto lentry = ldata[LEFT_CONSTANT ? 0 : base_idx];
							auto rentry = rdata[RIGHT_CONSTANT ? 0 : base_idx];
							result_data[base_idx] =
							    OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
							        fun, lentry, rentry, mask, base_idx);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
				auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
				    fun, lentry, rentry, mask, i);
			}
		}
	}
#endif

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteConstant(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC fun) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		FlatVector::SetSize(result, count);

		auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
		auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
		auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);

		if (ConstantVector::IsNull(left) || ConstantVector::IsNull(right)) {
			ConstantVector::SetNull(result, count_t(count));
			return;
		}
		*result_data = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
		    fun, *ldata, *rdata, ConstantVector::Validity(result), 0);
	}

#ifndef DUCKDB_SMALLER_BINARY
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static void ExecuteFlat(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC fun) {
		auto ldata = LEFT_CONSTANT ? ConstantVector::GetData<LEFT_TYPE>(left) : FlatVector::GetData<LEFT_TYPE>(left);
		auto rdata =
		    RIGHT_CONSTANT ? ConstantVector::GetData<RIGHT_TYPE>(right) : FlatVector::GetData<RIGHT_TYPE>(right);

		if ((LEFT_CONSTANT && ConstantVector::IsNull(left)) || (RIGHT_CONSTANT && ConstantVector::IsNull(right))) {
			// either left or right is constant NULL: result is constant NULL
			ConstantVector::SetNull(result, count_t(count));
			return;
		}

		result.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(result, count);
		auto result_data = FlatVector::GetDataMutable<RESULT_TYPE>(result);
		auto &result_validity = FlatVector::ValidityMutable(result);
		if (LEFT_CONSTANT) {
			if (OPWRAPPER::AddsNulls()) {
				result_validity.Copy(FlatVector::Validity(right), count);
			} else {
				FlatVector::SetValidity(result, FlatVector::Validity(right));
			}
		} else if (RIGHT_CONSTANT) {
			if (OPWRAPPER::AddsNulls()) {
				result_validity.Copy(FlatVector::Validity(left), count);
			} else {
				FlatVector::SetValidity(result, FlatVector::Validity(left));
			}
		} else {
			if (OPWRAPPER::AddsNulls()) {
				result_validity.Copy(FlatVector::Validity(left), count);
				if (result_validity.CannotHaveNull()) {
					result_validity.Copy(FlatVector::Validity(right), count);
				} else {
					result_validity.Combine(FlatVector::Validity(right), count);
				}
			} else {
				FlatVector::SetValidity(result, FlatVector::Validity(left));
				result_validity.Combine(FlatVector::Validity(right), count);
			}
		}
		ExecuteFlatLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, result_data, count, result_validity, fun);
	}
#endif

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteGenericLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                               RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
	                               const SelectionVector *__restrict rsel, idx_t count, const ValidityMask &lvalidity,
	                               const ValidityMask &rvalidity, ValidityMask &result_validity, FUNC fun) {
		if (lvalidity.CanHaveNull() || rvalidity.CanHaveNull()) {
			for (idx_t i = 0; i < count; i++) {
				auto lindex = lsel->get_index(i);
				auto rindex = rsel->get_index(i);
				if (lvalidity.RowIsValid(lindex) && rvalidity.RowIsValid(rindex)) {
					auto lentry = ldata[lindex];
					auto rentry = rdata[rindex];
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
					    fun, lentry, rentry, result_validity, i);
				} else {
					result_validity.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lentry = ldata[lsel->get_index(i)];
				auto rentry = rdata[rsel->get_index(i)];
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
				    fun, lentry, rentry, result_validity, i);
			}
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteGeneric(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC fun) {
		UnifiedVectorFormat ldata, rdata;

		left.ToUnifiedFormat(ldata);
		right.ToUnifiedFormat(rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::SetSize(result, count);
		auto result_data = FlatVector::GetDataMutable<RESULT_TYPE>(result);
		ExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(
		    UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata), UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata),
		    result_data, ldata.sel, rdata.sel, count, ldata.validity, rdata.validity,
		    FlatVector::ValidityMutable(result), fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteSwitch(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC fun) {
		auto left_vector_type = left.GetVectorType();
		auto right_vector_type = right.GetVectorType();
		if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(left, right, result, count, fun);
#ifndef DUCKDB_SMALLER_BINARY
		} else if (left_vector_type == VectorType::FLAT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, false, true>(left, right, result,
			                                                                                  count, fun);
		} else if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::FLAT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, true, false>(left, right, result,
			                                                                                  count, fun);
		} else if (left_vector_type == VectorType::FLAT_VECTOR && right_vector_type == VectorType::FLAT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, false, false>(left, right, result,
			                                                                                   count, fun);
#endif
		} else {
			ExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(left, right, result, count, fun);
		}
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(const Vector &left, const Vector &right, Vector &result, idx_t count, FUNC fun) {
		constexpr bool adds_nulls = std::is_same<decltype(fun(std::declval<LEFT_TYPE>(), std::declval<RIGHT_TYPE>())),
		                                         optional<RESULT_TYPE>>::value;
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryLambdaWrapper<adds_nulls>, bool, FUNC>(
		    left, right, result, count, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP,
	          class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static void Execute(const Vector &left, const Vector &right, Vector &result, idx_t count) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool>(left, right, result, count, false);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(const Vector &left, const Vector &right, Vector &result, idx_t count) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryStandardOperatorWrapper, OP, bool>(left, right, result,
		                                                                                           count, false);
	}

private:
	static idx_t GetExecuteCount(const Vector &left, const Vector &right) {
		const bool left_const = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
		const bool right_const = right.GetVectorType() == VectorType::CONSTANT_VECTOR;
		if (!left_const && !right_const && left.size() != right.size()) {
			throw InternalException(
			    "Mismatch in input vector sizes for BinaryExecutor - left has %d rows but right has %d", left.size(),
			    right.size());
		}
		return left_const ? right.size() : left.size();
	}

public:
	//! Convenience overloads without explicit count - count is derived from the input vectors.
	//! These add a size-mismatch check for non-constant vectors.
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(const Vector &left, const Vector &right, Vector &result, FUNC fun) {
		Execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right, result, GetExecuteCount(left, right), fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP,
	          class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static void Execute(const Vector &left, const Vector &right, Vector &result) {
		Execute<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, OPWRAPPER>(left, right, result, GetExecuteCount(left, right));
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(const Vector &left, const Vector &right, Vector &result) {
		ExecuteStandard<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, GetExecuteCount(left, right));
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectConstant(const Vector &left, const Vector &right, const SelectionVector &sel, idx_t count,
	                            SelectionVector *true_sel, SelectionVector *false_sel) {
		auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
		auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);

		// both sides are constant, return either 0 or the count
		// in this case we do not fill in the result selection vector at all
		if (ConstantVector::IsNull(left) || ConstantVector::IsNull(right) || !OP::Operation(*ldata, *rdata)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel.get_index(i));
				}
			}
			return 0;
		} else {
			if (true_sel) {
				for (idx_t i = 0; i < count; i++) {
					true_sel->set_index(i, sel.get_index(i));
				}
			}
			return count;
		}
	}

#ifndef DUCKDB_SMALLER_BINARY
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool HAS_TRUE_SEL,
	          bool HAS_FALSE_SEL>
	static inline idx_t SelectFlatLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                                   const SelectionVector &sel, idx_t count, const ValidityMask &validity_mask,
	                                   SelectionVector *true_sel, SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = validity_mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (ValidityMask::AllValid(validity_entry)) {
				// all valid: perform operation
				for (; base_idx < next; base_idx++) {
					idx_t result_idx = sel.get_index(base_idx);
					idx_t lidx = LEFT_CONSTANT ? 0 : base_idx;
					idx_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
					bool comparison_result = OP::Operation(ldata[lidx], rdata[ridx]);
					if (HAS_TRUE_SEL) {
						true_sel->set_index(true_count, result_idx);
						true_count += comparison_result;
					}
					if (HAS_FALSE_SEL) {
						false_sel->set_index(false_count, result_idx);
						false_count += !comparison_result;
					}
				}
			} else if (ValidityMask::NoneValid(validity_entry)) {
				// nothing valid: skip all
				if (HAS_FALSE_SEL) {
					for (; base_idx < next; base_idx++) {
						idx_t result_idx = sel.get_index(base_idx);
						false_sel->set_index(false_count, result_idx);
						false_count++;
					}
				}
				base_idx = next;
				continue;
			} else {
				// partially valid: need to check individual elements for validity
				idx_t start = base_idx;
				for (; base_idx < next; base_idx++) {
					idx_t result_idx = sel.get_index(base_idx);
					idx_t lidx = LEFT_CONSTANT ? 0 : base_idx;
					idx_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
					bool comparison_result = ValidityMask::RowIsValid(validity_entry, base_idx - start) &&
					                         OP::Operation(ldata[lidx], rdata[ridx]);
					if (HAS_TRUE_SEL) {
						true_sel->set_index(true_count, result_idx);
						true_count += comparison_result;
					}
					if (HAS_FALSE_SEL) {
						false_sel->set_index(false_count, result_idx);
						false_count += !comparison_result;
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

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static inline idx_t SelectFlatLoopSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                                         const SelectionVector &sel, idx_t count, const ValidityMask &mask,
	                                         SelectionVector *true_sel, SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true, true>(
			    ldata, rdata, sel, count, mask, true_sel, false_sel);
		} else if (true_sel) {
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true, false>(
			    ldata, rdata, sel, count, mask, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, false, true>(
			    ldata, rdata, sel, count, mask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static idx_t SelectFlat(const Vector &left, const Vector &right, const SelectionVector &sel, idx_t count,
	                        SelectionVector *true_sel, SelectionVector *false_sel) {
		auto ldata = LEFT_CONSTANT ? ConstantVector::GetData<LEFT_TYPE>(left) : FlatVector::GetData<LEFT_TYPE>(left);
		auto rdata =
		    RIGHT_CONSTANT ? ConstantVector::GetData<RIGHT_TYPE>(right) : FlatVector::GetData<RIGHT_TYPE>(right);

		if (LEFT_CONSTANT && ConstantVector::IsNull(left)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel.get_index(i));
				}
			}
			return 0;
		}
		if (RIGHT_CONSTANT && ConstantVector::IsNull(right)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel.get_index(i));
				}
			}
			return 0;
		}

		if (LEFT_CONSTANT) {
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, FlatVector::Validity(right), true_sel, false_sel);
		} else if (RIGHT_CONSTANT) {
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, FlatVector::Validity(left), true_sel, false_sel);
		} else {
			ValidityMask combined_mask = FlatVector::Validity(left);
			combined_mask.Combine(FlatVector::Validity(right), count);
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, combined_mask, true_sel, false_sel);
		}
	}

	template <class CONSTANT_TYPE, class GENERIC_TYPE, class OP, bool RIGHT_CONSTANT, bool CAN_HAVE_NULL,
	          bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static idx_t SelectGenericConstant(CONSTANT_TYPE constant, const GENERIC_TYPE *__restrict data,
	                                   const SelectionVector &generic_sel, const ValidityMask &mask,
	                                   const SelectionVector &result_sel, idx_t count, SelectionVector *true_sel,
	                                   SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t r = 0; r < count; r++) {
			auto result_idx = result_sel.get_index(r);
			auto idx = generic_sel.get_index(r);
			bool comparison_result = (!CAN_HAVE_NULL || mask.RowIsValid(idx));
			if constexpr (RIGHT_CONSTANT) {
				comparison_result = comparison_result && OP::Operation(data[idx], constant);
			} else {
				comparison_result = comparison_result && OP::Operation(constant, data[idx]);
			}
			if constexpr (HAS_TRUE_SEL) {
				true_sel->set_index(true_count, result_idx);
				true_count += comparison_result;
			}
			if constexpr (HAS_FALSE_SEL) {
				false_sel->set_index(false_count, result_idx);
				false_count += !comparison_result;
			}
		}
		if constexpr (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}

	template <class CONSTANT_TYPE, class GENERIC_TYPE, class OP, bool RIGHT_CONSTANT, bool CAN_HAVE_NULL>
	static idx_t SelectGenericConstant(CONSTANT_TYPE constant, const GENERIC_TYPE *__restrict data,
	                                   const SelectionVector &generic_sel, const ValidityMask &mask,
	                                   const SelectionVector &result_sel, idx_t count, SelectionVector *true_sel,
	                                   SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectGenericConstant<CONSTANT_TYPE, GENERIC_TYPE, OP, RIGHT_CONSTANT, CAN_HAVE_NULL, true, true>(
			    constant, data, generic_sel, mask, result_sel, count, true_sel, false_sel);
		} else if (true_sel) {
			return SelectGenericConstant<CONSTANT_TYPE, GENERIC_TYPE, OP, RIGHT_CONSTANT, CAN_HAVE_NULL, true, false>(
			    constant, data, generic_sel, mask, result_sel, count, true_sel, false_sel);
		} else if (false_sel) {
			return SelectGenericConstant<CONSTANT_TYPE, GENERIC_TYPE, OP, RIGHT_CONSTANT, CAN_HAVE_NULL, false, true>(
			    constant, data, generic_sel, mask, result_sel, count, true_sel, false_sel);
		} else {
			throw InternalException("Either true or false sel must be set");
		}
	}

	template <class CONSTANT_TYPE, class GENERIC_TYPE, class OP, bool RIGHT_CONSTANT>
	static idx_t SelectGenericConstant(CONSTANT_TYPE constant, const GENERIC_TYPE *__restrict data,
	                                   const SelectionVector &generic_sel, const ValidityMask &mask,
	                                   const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                                   SelectionVector *false_sel) {
		if (mask.CanHaveNull()) {
			return SelectGenericConstant<CONSTANT_TYPE, GENERIC_TYPE, OP, RIGHT_CONSTANT, true>(
			    constant, data, generic_sel, mask, sel, count, true_sel, false_sel);
		} else {
			return SelectGenericConstant<CONSTANT_TYPE, GENERIC_TYPE, OP, RIGHT_CONSTANT, false>(
			    constant, data, generic_sel, mask, sel, count, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool RIGHT_CONSTANT>
	static idx_t SelectGenericConstant(const Vector &left, const Vector &right, const SelectionVector &sel, idx_t count,
	                                   SelectionVector *true_sel, SelectionVector *false_sel) {
		constexpr bool LEFT_CONSTANT = !RIGHT_CONSTANT;
		if (LEFT_CONSTANT && ConstantVector::IsNull(left)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel.get_index(i));
				}
			}
			return 0;
		}
		if (RIGHT_CONSTANT && ConstantVector::IsNull(right)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel.get_index(i));
				}
			}
			return 0;
		}

		UnifiedVectorFormat format;
		if (LEFT_CONSTANT) {
		} else {
			right.ToUnifiedFormat(format);
		}

		if (LEFT_CONSTANT) {
			right.ToUnifiedFormat(format);
			auto data = UnifiedVectorFormat::GetData<RIGHT_TYPE>(format);
			return SelectGenericConstant<LEFT_TYPE, RIGHT_TYPE, OP, RIGHT_CONSTANT>(
			    *ConstantVector::GetData<LEFT_TYPE>(left), data, *format.sel, format.validity, sel, count, true_sel,
			    false_sel);
		} else {
			left.ToUnifiedFormat(format);
			auto data = UnifiedVectorFormat::GetData<LEFT_TYPE>(format);
			return SelectGenericConstant<RIGHT_TYPE, LEFT_TYPE, OP, RIGHT_CONSTANT>(
			    *ConstantVector::GetData<RIGHT_TYPE>(right), data, *format.sel, format.validity, sel, count, true_sel,
			    false_sel);
		}
	}
#endif

#ifndef DUCKDB_SMALLER_BINARY
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
#else
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
#endif
	static inline idx_t SelectGenericLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                                      const SelectionVector *__restrict lsel,
	                                      const SelectionVector *__restrict rsel, const SelectionVector &result_sel,
	                                      idx_t count, const ValidityMask &lvalidity, const ValidityMask &rvalidity,
	                                      SelectionVector *true_sel, SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
#ifdef DUCKDB_SMALLER_BINARY
		const bool HAS_TRUE_SEL = true_sel;
		const bool HAS_FALSE_SEL = false_sel;
		const bool NO_NULL = false;
#endif
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel.get_index(i);
			auto lindex = lsel->get_index(i);
			auto rindex = rsel->get_index(i);
			if ((NO_NULL || (lvalidity.RowIsValid(lindex) && rvalidity.RowIsValid(rindex))) &&
			    OP::Operation(ldata[lindex], rdata[rindex])) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
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
	SelectGenericLoopSelSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                           const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
	                           const SelectionVector &result_sel, idx_t count, const ValidityMask &lvalidity,
	                           const ValidityMask &rvalidity, SelectionVector *true_sel, SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		} else if (true_sel) {
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		}
	}
#endif

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static inline idx_t
	SelectGenericLoopSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
	                        const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
	                        const SelectionVector &result_sel, idx_t count, const ValidityMask &lvalidity,
	                        const ValidityMask &rvalidity, SelectionVector *true_sel, SelectionVector *false_sel) {
#ifndef DUCKDB_SMALLER_BINARY
		if (lvalidity.CanHaveNull() || rvalidity.CanHaveNull()) {
			return SelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		} else {
			return SelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		}
#else
		return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP>(ldata, rdata, lsel, rsel, result_sel, count, lvalidity,
		                                                    rvalidity, true_sel, false_sel);
#endif
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectGeneric(const Vector &left, const Vector &right, const SelectionVector &sel, idx_t count,
	                           SelectionVector *true_sel, SelectionVector *false_sel) {
		UnifiedVectorFormat ldata, rdata;

		left.ToUnifiedFormat(ldata);
		right.ToUnifiedFormat(rdata);

		return SelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>(
		    UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata), UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata), ldata.sel,
		    rdata.sel, sel, count, ldata.validity, rdata.validity, true_sel, false_sel);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t Select(const Vector &left, const Vector &right, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!sel) {
			sel = FlatVector::IncrementalSelectionVector();
		}
		if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return SelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, *sel, count, true_sel, false_sel);
#ifndef DUCKDB_SMALLER_BINARY
		} else if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		           right.GetVectorType() == VectorType::FLAT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, *sel, count, true_sel, false_sel);
		} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
		           right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, *sel, count, true_sel, false_sel);
		} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
		           right.GetVectorType() == VectorType::FLAT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, *sel, count, true_sel, false_sel);
		} else if (left.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return SelectGenericConstant<LEFT_TYPE, RIGHT_TYPE, OP, false>(left, right, *sel, count, true_sel,
			                                                               false_sel);
		} else if (right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return SelectGenericConstant<LEFT_TYPE, RIGHT_TYPE, OP, true>(left, right, *sel, count, true_sel,
			                                                              false_sel);
#endif
		} else {
			return SelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, *sel, count, true_sel, false_sel);
		}
	}
};

} // namespace duckdb
