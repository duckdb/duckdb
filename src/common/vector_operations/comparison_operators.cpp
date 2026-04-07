//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/likely.hpp"

namespace duckdb {

template <class T>
static bool EqualsFloat(T left, T right) {
	if (DUCKDB_UNLIKELY(Value::IsNan(left) && Value::IsNan(right))) {
		return true;
	}
	return left == right;
}

template <>
bool Equals::Operation(const float &left, const float &right) {
	return EqualsFloat<float>(left, right);
}

template <>
bool Equals::Operation(const double &left, const double &right) {
	return EqualsFloat<double>(left, right);
}

template <class T>
static bool GreaterThanFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no number that is bigger than right
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return false;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left > right;
}

template <>
bool GreaterThan::Operation(const float &left, const float &right) {
	return GreaterThanFloat<float>(left, right);
}

template <>
bool GreaterThan::Operation(const double &left, const double &right) {
	return GreaterThanFloat<double>(left, right);
}

template <class T>
static bool GreaterThanEqualsFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no bigger number
	// we only return true if left is also nan (in which case the numbers are equal)
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return left_is_nan;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left >= right;
}

template <>
bool GreaterThanEquals::Operation(const float &left, const float &right) {
	return GreaterThanEqualsFloat<float>(left, right);
}

template <>
bool GreaterThanEquals::Operation(const double &left, const double &right) {
	return GreaterThanEqualsFloat<double>(left, right);
}

template <class T>
static int8_t ComparatorFloat(T left, T right) {
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	if (DUCKDB_UNLIKELY(left_is_nan || right_is_nan)) {
		if (left_is_nan && right_is_nan) {
			return 0;
		}
		return left_is_nan ? 1 : -1;
	}
	if (left < right) {
		return -1;
	}
	if (left > right) {
		return 1;
	}
	return 0;
}

template <>
int8_t Comparator::Operation(const float &left, const float &right) {
	return ComparatorFloat<float>(left, right);
}

template <>
int8_t Comparator::Operation(const double &left, const double &right) {
	return ComparatorFloat<double>(left, right);
}

namespace {
struct ComparisonSelector {
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel, ValidityMask &null_mask) {
		throw NotImplementedException("Unknown comparison operation!");
	}
};

template <>
inline idx_t ComparisonSelector::Select<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel, ValidityMask &null_mask) {
	return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel, &null_mask);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector *false_sel, ValidityMask &null_mask) {
	return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel, &null_mask);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                             idx_t count, SelectionVector *true_sel,
                                                             SelectionVector *false_sel, ValidityMask &null_mask) {
	return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel, &null_mask);
}

template <>
inline idx_t
ComparisonSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                      idx_t count, SelectionVector *true_sel,
                                                      SelectionVector *false_sel, ValidityMask &null_mask) {
	return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel, &null_mask);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                          idx_t count, SelectionVector *true_sel,
                                                          SelectionVector *false_sel, ValidityMask &null_mask) {
	return VectorOperations::GreaterThan(right, left, sel, count, true_sel, false_sel, &null_mask);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                                idx_t count, SelectionVector *true_sel,
                                                                SelectionVector *false_sel, ValidityMask &null_mask) {
	return VectorOperations::GreaterThanEquals(right, left, sel, count, true_sel, false_sel, &null_mask);
}

static void ComparesNotNull(UnifiedVectorFormat &ldata, UnifiedVectorFormat &rdata, ValidityMask &vresult,
                            idx_t count) {
	for (idx_t i = 0; i < count; ++i) {
		auto lidx = ldata.sel->get_index(i);
		auto ridx = rdata.sel->get_index(i);
		if (!ldata.validity.RowIsValid(lidx) || !rdata.validity.RowIsValid(ridx)) {
			vresult.SetInvalid(i);
		}
	}
}

template <typename OP>
static void NestedComparisonExecutor(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if ((left_constant && ConstantVector::IsNull(left)) || (right_constant && ConstantVector::IsNull(right))) {
		// either left or right is constant NULL: result is constant NULL
		ConstantVector::SetNull(result);
		return;
	}

	if (left_constant && right_constant) {
		// both sides are constant, and neither is NULL so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto &result_validity = ConstantVector::Validity(result);
		SelectionVector true_sel(1);
		auto match_count = ComparisonSelector::Select<OP>(left, right, nullptr, 1, &true_sel, nullptr, result_validity);
		// since we are dealing with nested types where the values are not NULL, the result is always valid (i.e true or
		// false)
		result_validity.SetAllValid(1);
		auto result_data = ConstantVector::GetData<bool>(result);
		result_data[0] = match_count > 0;
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat leftv, rightv;
	left.ToUnifiedFormat(count, leftv);
	right.ToUnifiedFormat(count, rightv);
	if (leftv.validity.CanHaveNull() || rightv.validity.CanHaveNull()) {
		ComparesNotNull(leftv, rightv, result_validity, count);
	}
	ValidityMask original_mask;
	original_mask.SetAllValid(count);
	original_mask.Copy(result_validity, count);

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);
	idx_t match_count =
	    ComparisonSelector::Select<OP>(left, right, nullptr, count, &true_sel, &false_sel, result_validity);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
		// if the row was valid during the null check, set it to valid here as well
		if (original_mask.RowIsValid(idx)) {
			result_validity.SetValid(idx);
		}
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
		if (original_mask.RowIsValid(idx)) {
			result_validity.SetValid(idx);
		}
	}
}

struct ComparisonExecutor {
private:
	template <class T, class OP>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, bool, OP>(left, right, result, count);
	}

public:
	template <class OP>
	static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		D_ASSERT(left.GetType().InternalType() == right.GetType().InternalType() &&
		         result.GetType() == LogicalType::BOOLEAN);
		// the inplace loops take the result as the last parameter
		switch (left.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedExecute<int8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT16:
			TemplatedExecute<int16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT32:
			TemplatedExecute<int32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT64:
			TemplatedExecute<int64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT8:
			TemplatedExecute<uint8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT16:
			TemplatedExecute<uint16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT32:
			TemplatedExecute<uint32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT64:
			TemplatedExecute<uint64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT128:
			TemplatedExecute<hugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT128:
			TemplatedExecute<uhugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedExecute<float, OP>(left, right, result, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedExecute<double, OP>(left, right, result, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedExecute<interval_t, OP>(left, right, result, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedExecute<string_t, OP>(left, right, result, count);
			break;
		case PhysicalType::LIST:
		case PhysicalType::STRUCT:
		case PhysicalType::ARRAY:
			NestedComparisonExecutor<OP>(left, right, result, count);
			break;
		default:
			throw InternalException("Invalid type for comparison");
		}
	}
};
} // namespace

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::Equals>(left, right, result, count);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::NotEquals>(left, right, result, count);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(left, right, result, count);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(right, left, result, count);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(left, right, result, count);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(right, left, result, count);
}

struct StandardComparatorExecute {
	template <class T>
	static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, int8_t, duckdb::Comparator>(left, right, result, count);
	}
};

struct DistinctComparatorExecute {
	template <class T>
	static void Execute(Vector &left, Vector &right, int8_t *result_data, idx_t count,
	                    const SelectionVector &sel, idx_t sel_count) {
		auto left_values = left.Values<T>(count);
		auto right_values = right.Values<T>(count);
		for (idx_t i = 0; i < sel_count; i++) {
			auto idx = sel.get_index(i);
			auto lentry = left_values[idx];
			auto rentry = right_values[idx];
			result_data[idx] =
			    duckdb::DistinctComparator::Operation<T>(lentry.value, rentry.value, !lentry.IsValid(), !rentry.IsValid());
		}
	}
};

// forward declaration - StructComparator calls DistinctComparator recursively for children
static void DistinctComparatorTypeSwitch(Vector &left, Vector &right, int8_t *result_data, idx_t count,
                                         const SelectionVector &sel, idx_t sel_count);

template <bool IS_DISTINCT>
static void StructComparator(Vector &left, Vector &right, int8_t *result_data, idx_t count,
                             const SelectionVector &sel, idx_t sel_count) {
	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	// step 1: handle struct-level validity
	auto left_validity = left.Validity(count);
	auto right_validity = right.Validity(count);
	bool has_nulls = left_validity.CanHaveNull() || right_validity.CanHaveNull();

	// initialize the selection vector of remaining (undecided) rows
	SelectionVector remaining_sel(sel_count);
	idx_t remaining_count;
	if (!has_nulls) {
		// no nulls - all selected rows need child comparison
		remaining_sel.Initialize(sel);
		remaining_count = sel_count;
		for (idx_t i = 0; i < sel_count; i++) {
			result_data[sel.get_index(i)] = 0;
		}
	} else {
		remaining_count = 0;
		for (idx_t i = 0; i < sel_count; i++) {
			auto idx = sel.get_index(i);
			bool left_null = !left_validity.IsValid(idx);
			bool right_null = !right_validity.IsValid(idx);
			if (left_null || right_null) {
				if (IS_DISTINCT) {
					// NULLS LAST: NULL == NULL → 0, NULL > non-NULL → 1, non-NULL < NULL → -1
					result_data[idx] = (left_null && right_null) ? 0 : (left_null ? 1 : -1);
				} else {
					// regular comparator: NULL propagates as NULL in the result
					// result_data value is unused, caller must set validity
					result_data[idx] = 0;
				}
			} else {
				result_data[idx] = 0;
				remaining_sel.set_index(remaining_count++, idx);
			}
		}
	}

	// step 2: compare child vectors one by one
	// once a child produces a non-zero result for a row, that row is resolved
	for (idx_t child_idx = 0; child_idx < lchildren.size() && remaining_count > 0; child_idx++) {
		// always use DistinctComparator for struct children
		DistinctComparatorTypeSwitch(lchildren[child_idx], rchildren[child_idx], result_data, count, remaining_sel,
		                             remaining_count);

		// partition remaining into resolved vs still-undecided
		idx_t new_remaining_count = 0;
		for (idx_t i = 0; i < remaining_count; i++) {
			auto row_idx = remaining_sel.get_index(i);
			if (result_data[row_idx] == 0) {
				remaining_sel.set_index(new_remaining_count++, row_idx);
			}
		}
		remaining_count = new_remaining_count;
	}
}

template <bool IS_DISTINCT>
static void DistinctComparatorTypeSwitchInternal(Vector &left, Vector &right, int8_t *result_data, idx_t count,
                                                 const SelectionVector &sel, idx_t sel_count) {
	D_ASSERT(left.GetType().InternalType() == right.GetType().InternalType());
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		DistinctComparatorExecute::Execute<int8_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::INT16:
		DistinctComparatorExecute::Execute<int16_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::INT32:
		DistinctComparatorExecute::Execute<int32_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::INT64:
		DistinctComparatorExecute::Execute<int64_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::UINT8:
		DistinctComparatorExecute::Execute<uint8_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::UINT16:
		DistinctComparatorExecute::Execute<uint16_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::UINT32:
		DistinctComparatorExecute::Execute<uint32_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::UINT64:
		DistinctComparatorExecute::Execute<uint64_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::INT128:
		DistinctComparatorExecute::Execute<hugeint_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::UINT128:
		DistinctComparatorExecute::Execute<uhugeint_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::FLOAT:
		DistinctComparatorExecute::Execute<float>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::DOUBLE:
		DistinctComparatorExecute::Execute<double>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::INTERVAL:
		DistinctComparatorExecute::Execute<interval_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::VARCHAR:
		DistinctComparatorExecute::Execute<string_t>(left, right, result_data, count, sel, sel_count);
		break;
	case PhysicalType::STRUCT:
		StructComparator<IS_DISTINCT>(left, right, result_data, count, sel, sel_count);
		break;
	default:
		throw InternalException("Invalid type for comparator");
	}
}

static void DistinctComparatorTypeSwitch(Vector &left, Vector &right, int8_t *result_data, idx_t count,
                                         const SelectionVector &sel, idx_t sel_count) {
	DistinctComparatorTypeSwitchInternal<true>(left, right, result_data, count, sel, sel_count);
}

static void ComparatorTypeSwitch(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType().InternalType() == right.GetType().InternalType() &&
	         result.GetType() == LogicalType::TINYINT);
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		StandardComparatorExecute::Execute<int8_t>(left, right, result, count);
		break;
	case PhysicalType::INT16:
		StandardComparatorExecute::Execute<int16_t>(left, right, result, count);
		break;
	case PhysicalType::INT32:
		StandardComparatorExecute::Execute<int32_t>(left, right, result, count);
		break;
	case PhysicalType::INT64:
		StandardComparatorExecute::Execute<int64_t>(left, right, result, count);
		break;
	case PhysicalType::UINT8:
		StandardComparatorExecute::Execute<uint8_t>(left, right, result, count);
		break;
	case PhysicalType::UINT16:
		StandardComparatorExecute::Execute<uint16_t>(left, right, result, count);
		break;
	case PhysicalType::UINT32:
		StandardComparatorExecute::Execute<uint32_t>(left, right, result, count);
		break;
	case PhysicalType::UINT64:
		StandardComparatorExecute::Execute<uint64_t>(left, right, result, count);
		break;
	case PhysicalType::INT128:
		StandardComparatorExecute::Execute<hugeint_t>(left, right, result, count);
		break;
	case PhysicalType::UINT128:
		StandardComparatorExecute::Execute<uhugeint_t>(left, right, result, count);
		break;
	case PhysicalType::FLOAT:
		StandardComparatorExecute::Execute<float>(left, right, result, count);
		break;
	case PhysicalType::DOUBLE:
		StandardComparatorExecute::Execute<double>(left, right, result, count);
		break;
	case PhysicalType::INTERVAL:
		StandardComparatorExecute::Execute<interval_t>(left, right, result, count);
		break;
	case PhysicalType::VARCHAR:
		StandardComparatorExecute::Execute<string_t>(left, right, result, count);
		break;
	case PhysicalType::STRUCT: {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<int8_t>(result);
		SelectionVector sel(count);
		sel.Initialize(*FlatVector::IncrementalSelectionVector());
		StructComparator<false>(left, right, result_data, count, sel, count);
		break;
	}
	default:
		throw InternalException("Invalid type for comparator");
	}
}

void VectorOperations::Comparator(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparatorTypeSwitch(left, right, result, count);
}

void VectorOperations::DistinctComparator(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::TINYINT);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int8_t>(result);
	SelectionVector sel(count);
	sel.Initialize(*FlatVector::IncrementalSelectionVector());
	DistinctComparatorTypeSwitchInternal<true>(left, right, result_data, count, sel, count);
}

} // namespace duckdb
