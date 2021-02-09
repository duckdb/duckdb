#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

struct DistinctBinaryLambdaWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, bool is_left_null,
	                                    bool is_right_null) {
		return OP::template Operation<LEFT_TYPE>(left, right, is_left_null, is_right_null);
	}
};

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL,
          bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static void DistinctExecuteFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                    RESULT_TYPE *__restrict result_data, idx_t count, nullmask_t &nullmask_left,
                                    nullmask_t &nullmask_right, FUNC fun) {
	if (!LEFT_CONSTANT) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	}
	if (!RIGHT_CONSTANT) {
		ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
	}
	for (idx_t i = 0; i < count; i++) {
		auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
		auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
		result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
		    fun, lentry, rentry, nullmask_left[i], nullmask_right[i]);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
static void DistinctExecuteConstant(Vector &left, Vector &right, Vector &result, FUNC fun) {
	result.buffer->vector_type = VectorType::CONSTANT_VECTOR;

	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
	auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
	*result_data = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
	    fun, *ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right));
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL,
          bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static void DistinctExecuteFlat(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
	auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
	auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);
	result.buffer->vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
	if (LEFT_CONSTANT) {
		nullmask_t constant_nullmask;
		if (ConstantVector::IsNull(left)) {
			constant_nullmask.set();
		} else {
			constant_nullmask.reset();
		}
		return DistinctExecuteFlatLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL,
		                               LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, result_data, count, constant_nullmask, FlatVector::Nullmask(right), fun);
	} else if (RIGHT_CONSTANT) {
		nullmask_t constant_nullmask;
		if (ConstantVector::IsNull(right)) {
			constant_nullmask.set();
		} else {
			constant_nullmask.reset();
		}
		return DistinctExecuteFlatLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL,
		                               LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, result_data, count, FlatVector::Nullmask(left), constant_nullmask, fun);
	} else {
		DistinctExecuteFlatLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, LEFT_CONSTANT,
		                        RIGHT_CONSTANT>(ldata, rdata, result_data, count, FlatVector::Nullmask(left),
		                                        FlatVector::Nullmask(right), fun);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
static void DistinctExecuteGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                       RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
                                       const SelectionVector *__restrict rsel, idx_t count, nullmask_t &lnullmask,
                                       nullmask_t &rnullmask, nullmask_t &result_nullmask, FUNC fun) {
	for (idx_t i = 0; i < count; i++) {
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		auto lentry = ldata[lindex];
		auto rentry = rdata[rindex];
		result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
		    fun, lentry, rentry, lnullmask[lindex], rnullmask[rindex]);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
static void DistinctExecuteGeneric(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
	VectorData ldata, rdata;

	left.Orrify(count, ldata);
	right.Orrify(count, rdata);

	result.buffer->vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
	DistinctExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
	    (LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data, result_data, ldata.sel, rdata.sel, count, *ldata.nullmask,
	    *rdata.nullmask, FlatVector::Nullmask(result), fun);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC, bool IGNORE_NULL>
static void DistinctExecuteSwitch(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
	if (left.buffer->vector_type == VectorType::CONSTANT_VECTOR &&
	    right.buffer->vector_type == VectorType::CONSTANT_VECTOR) {
		DistinctExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(left, right,
		                                                                                              result, fun);
	} else if (left.buffer->vector_type == VectorType::FLAT_VECTOR &&
	           right.buffer->vector_type == VectorType::CONSTANT_VECTOR) {
		DistinctExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false, true>(
		    left, right, result, count, fun);
	} else if (left.buffer->vector_type == VectorType::CONSTANT_VECTOR &&
	           right.buffer->vector_type == VectorType::FLAT_VECTOR) {
		DistinctExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, true, false>(
		    left, right, result, count, fun);
	} else if (left.buffer->vector_type == VectorType::FLAT_VECTOR &&
	           right.buffer->vector_type == VectorType::FLAT_VECTOR) {
		DistinctExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL, false, false>(
		    left, right, result, count, fun);
	} else {
		DistinctExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, IGNORE_NULL>(
		    left, right, result, count, fun);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false,
          class OPWRAPPER = DistinctBinaryLambdaWrapper>
static void DistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool, IGNORE_NULL>(left, right, result,
	                                                                                            count, false);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t
DistinctSelectGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                          const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                          const SelectionVector *__restrict result_sel, idx_t count, nullmask_t &lnullmask,
                          nullmask_t &rnullmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = result_sel->get_index(i);
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		if (NO_NULL) {
			if (OP::Operation(ldata[lindex], rdata[rindex], true, true) && HAS_TRUE_SEL) {
				true_sel->set_index(true_count++, result_idx);
			}
		} else {
			if (OP::Operation(ldata[lindex], rdata[rindex], lnullmask[i], rnullmask[i]) && HAS_FALSE_SEL) {
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
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL>
static inline idx_t
DistinctSelectGenericLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                   const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                   const SelectionVector *__restrict result_sel, idx_t count, nullmask_t &lnullmask,
                                   nullmask_t &rnullmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static inline idx_t
DistinctSelectGenericLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                const SelectionVector *__restrict result_sel, idx_t count, nullmask_t &lnullmask,
                                nullmask_t &rnullmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (lnullmask.any() || rnullmask.any()) {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
	} else {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lnullmask, rnullmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectGeneric(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	VectorData ldata, rdata;

	left.Orrify(count, ldata);
	right.Orrify(count, rdata);

	return DistinctSelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>((LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data,
	                                                                  ldata.sel, rdata.sel, sel, count, *ldata.nullmask,
	                                                                  *rdata.nullmask, true_sel, false_sel);
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL,
          bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DistinctSelectFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                           const SelectionVector *sel, idx_t count, nullmask_t &nullmask_left,
                                           nullmask_t &nullmask_right, SelectionVector *true_sel,
                                           SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t result_idx = sel->get_index(i);
		idx_t lidx = LEFT_CONSTANT ? 0 : i;
		idx_t ridx = RIGHT_CONSTANT ? 0 : i;
		bool comparison_result = OP::Operation(ldata[lidx], rdata[ridx], nullmask_left[i], nullmask_right[i]);
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
                                                    const SelectionVector *sel, idx_t count, nullmask_t &nullmask_left,
                                                    nullmask_t &nullmask_right, SelectionVector *true_sel,
                                                    SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, true>(
		    ldata, rdata, sel, count, nullmask_left, nullmask_right, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, false>(
		    ldata, rdata, sel, count, nullmask_left, nullmask_right, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, false, true>(
		    ldata, rdata, sel, count, nullmask_left, nullmask_right, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static inline idx_t DistinctSelectFlatLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                 const SelectionVector *sel, idx_t count, nullmask_t &nullmask_left,
                                                 nullmask_t &nullmask_right, SelectionVector *true_sel,
                                                 SelectionVector *false_sel) {
	return DistinctSelectFlatLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true>(
	    ldata, rdata, sel, count, nullmask_left, nullmask_right, true_sel, false_sel);
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static idx_t DistinctSelectFlat(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
	auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);
	if (LEFT_CONSTANT) {
		nullmask_t constant_nullmask;
		if (ConstantVector::IsNull(left)) {
			constant_nullmask.set();
		} else {
			constant_nullmask.reset();
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, constant_nullmask, FlatVector::Nullmask(right), true_sel, false_sel);
	} else if (RIGHT_CONSTANT) {
		nullmask_t constant_nullmask;
		if (ConstantVector::IsNull(right)) {
			constant_nullmask.set();
		} else {
			constant_nullmask.reset();
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Nullmask(left), constant_nullmask, true_sel, false_sel);
	} else {
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Nullmask(left), FlatVector::Nullmask(right), true_sel, false_sel);
	}
}
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

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelect(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                            SelectionVector *true_sel, SelectionVector *false_sel) {
	if (!sel) {
		sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	}
	if (left.buffer->vector_type == VectorType::CONSTANT_VECTOR &&
	    right.buffer->vector_type == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	} else if (left.buffer->vector_type == VectorType::CONSTANT_VECTOR &&
	           right.buffer->vector_type == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, sel, count, true_sel, false_sel);
	} else if (left.buffer->vector_type == VectorType::FLAT_VECTOR &&
	           right.buffer->vector_type == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, sel, count, true_sel, false_sel);
	} else if (left.buffer->vector_type == VectorType::FLAT_VECTOR &&
	           right.buffer->vector_type == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, sel, count, true_sel,
		                                                                   false_sel);
	} else {
		return DistinctSelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	}
}
template <class T, class OP, bool IGNORE_NULL = false>
static inline void TemplatedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecute<T, T, bool, OP, IGNORE_NULL>(left, right, result, count);
}
template <class OP>
static void ExecuteDistinct(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.buffer->type == right.buffer->type && result.buffer->type == LogicalType::BOOLEAN);
	// the inplace loops take the result as the last parameter
	switch (left.buffer->type.InternalType()) {
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
	case PhysicalType::POINTER:
		TemplatedDistinctExecute<uintptr_t, OP>(left, right, result, count);
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
		TemplatedDistinctExecute<string_t, OP, true>(left, right, result, count);
		break;
	default:
		throw InvalidTypeException(left.buffer->type, "Invalid type for distinct comparison");
	}
}

template <class OP>
static idx_t TemplatedDistinctSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                              SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.buffer->type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return DistinctSelect<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return DistinctSelect<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return DistinctSelect<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return DistinctSelect<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return DistinctSelect<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return DistinctSelect<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return DistinctSelect<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return DistinctSelect<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return DistinctSelect<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::POINTER:
		return DistinctSelect<uintptr_t, uintptr_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return DistinctSelect<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return DistinctSelect<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return DistinctSelect<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return DistinctSelect<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InvalidTypeException(left.buffer->type, "Invalid type for comparison");
	}
}

void VectorOperations::DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::DistinctFrom>(left, right, result, count);
}

void VectorOperations::NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::NotDistinctFrom>(left, right, result, count);
}

// result = A != B with nulls being equal
idx_t VectorOperations::SelectDistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                           SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, true_sel, false_sel);
}
// result = A == B with nulls being equal
idx_t VectorOperations::SelectNotDistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                              SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::NotDistinctFrom>(left, right, sel, count, true_sel, false_sel);
}

} // namespace duckdb
