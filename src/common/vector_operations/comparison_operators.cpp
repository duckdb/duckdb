//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
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
			return Comparator::VALUES_ARE_EQUAL;
		}
		// NaN is bigger than anything
		return left_is_nan ? Comparator::LEFT_IS_GREATER : Comparator::RIGHT_IS_GREATER;
	}
	if (left < right) {
		return Comparator::RIGHT_IS_GREATER;
	}
	if (left > right) {
		return Comparator::LEFT_IS_GREATER;
	}
	return Comparator::VALUES_ARE_EQUAL;
}

template <>
int8_t Comparator::Operation(const float &left, const float &right) {
	return ComparatorFloat<float>(left, right);
}

template <>
int8_t Comparator::Operation(const double &left, const double &right) {
	return ComparatorFloat<double>(left, right);
}

//===--------------------------------------------------------------------===//
// Fast path: direct BinaryExecutor for primitive types (single pass, no intermediate vector)
//===--------------------------------------------------------------------===//
template <class OP>
static bool TryPrimitiveComparisonExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
#ifdef DUCKDB_SMALLER_BINARY
	return false;
#else
	D_ASSERT(left.GetType().InternalType() == right.GetType().InternalType());
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		BinaryExecutor::Execute<int8_t, int8_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT16:
		BinaryExecutor::Execute<int16_t, int16_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT32:
		BinaryExecutor::Execute<int32_t, int32_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT64:
		BinaryExecutor::Execute<int64_t, int64_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT8:
		BinaryExecutor::Execute<uint8_t, uint8_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT16:
		BinaryExecutor::Execute<uint16_t, uint16_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT32:
		BinaryExecutor::Execute<uint32_t, uint32_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT64:
		BinaryExecutor::Execute<uint64_t, uint64_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT128:
		BinaryExecutor::Execute<hugeint_t, hugeint_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT128:
		BinaryExecutor::Execute<uhugeint_t, uhugeint_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::FLOAT:
		BinaryExecutor::Execute<float, float, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::DOUBLE:
		BinaryExecutor::Execute<double, double, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::INTERVAL:
		BinaryExecutor::Execute<interval_t, interval_t, bool, OP>(left, right, result, count);
		return true;
	case PhysicalType::VARCHAR:
		BinaryExecutor::Execute<string_t, string_t, bool, OP>(left, right, result, count);
		return true;
	default:
		return false;
	}
#endif
}

template <class PREDICATE>
static void ComparatorToBoolean(Vector &left, Vector &right, Vector &result, idx_t count, PREDICATE predicate) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);
	Vector comparator_result(LogicalType::TINYINT, count);
	VectorOperations::Comparator(left, right, comparator_result, count);
	auto cmp_data = comparator_result.Values<int8_t>(count);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::Writer<bool>(result, count);
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto entry = cmp_data[i];
		if (!entry.IsValid()) {
			result_validity.SetInvalid(i);
		} else {
			result_data[i] = predicate(entry.value);
		}
	}
}

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (TryPrimitiveComparisonExecute<duckdb::Equals>(left, right, result, count)) {
		return;
	}
	ComparatorToBoolean(left, right, result, count, [](int8_t v) { return v == 0; });
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (TryPrimitiveComparisonExecute<duckdb::NotEquals>(left, right, result, count)) {
		return;
	}
	ComparatorToBoolean(left, right, result, count, [](int8_t v) { return v != 0; });
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (TryPrimitiveComparisonExecute<duckdb::GreaterThan>(left, right, result, count)) {
		return;
	}
	ComparatorToBoolean(left, right, result, count, [](int8_t v) { return v > 0; });
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (TryPrimitiveComparisonExecute<duckdb::GreaterThanEquals>(left, right, result, count)) {
		return;
	}
	ComparatorToBoolean(left, right, result, count, [](int8_t v) { return v >= 0; });
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	// NOLINTNEXTLINE: flip right / left (left < right is equal to right > left)
	if (TryPrimitiveComparisonExecute<duckdb::GreaterThan>(right, left, result, count)) {
		return;
	}
	ComparatorToBoolean(left, right, result, count, [](int8_t v) { return v < 0; });
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	// NOLINTNEXTLINE: flip right / left (left <= right is equal to right >= left)
	if (TryPrimitiveComparisonExecute<duckdb::GreaterThanEquals>(right, left, result, count)) {
		return;
	}
	ComparatorToBoolean(left, right, result, count, [](int8_t v) { return v <= 0; });
}

struct StandardComparatorExecute {
	template <class T>
	static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, int8_t, duckdb::Comparator>(left, right, result, count);
	}
};

struct DistinctComparatorExecute {
	template <class T>
	static void Execute(Vector &left, Vector &right, int8_t *result_data, const SelectionVector &lhs_sel,
	                    const SelectionVector &rhs_sel, idx_t sel_count) {
		UnifiedVectorFormat left_format, right_format;
		left.ToUnifiedFormat(sel_count, left_format);
		right.ToUnifiedFormat(sel_count, right_format);
		auto ldata = UnifiedVectorFormat::GetData<T>(left_format);
		auto rdata = UnifiedVectorFormat::GetData<T>(right_format);
		for (idx_t i = 0; i < sel_count; i++) {
			auto lidx = left_format.sel->get_index(lhs_sel.get_index(i));
			auto ridx = right_format.sel->get_index(rhs_sel.get_index(i));
			bool left_null = !left_format.validity.RowIsValid(lidx);
			bool right_null = !right_format.validity.RowIsValid(ridx);
			result_data[i] = duckdb::DistinctComparator::Operation<T>(ldata[lidx], rdata[ridx], left_null, right_null);
		}
	}
};

// forward declaration - nested comparators call DistinctComparator recursively for children
static void DistinctComparatorTypeSwitch(Vector &left, Vector &right, int8_t *result_data,
                                         const SelectionVector &lhs_sel, const SelectionVector &rhs_sel,
                                         idx_t sel_count);

static int8_t DistinctNullComparator(bool left_null, bool right_null) {
	if (left_null && right_null) {
		return Comparator::VALUES_ARE_EQUAL;
	}
	// default is NULLS LAST - i.e. NULL is greater than every non-NULL value
	if (left_null) {
		return Comparator::LEFT_IS_GREATER;
	}
	return Comparator::RIGHT_IS_GREATER;
}

static void StructComparator(Vector &left, Vector &right, int8_t *result_data, const SelectionVector &lhs_sel,
                             const SelectionVector &rhs_sel, idx_t sel_count,
                             optional_ptr<ValidityMask> result_validity = nullptr) {
	if (sel_count == 0) {
		return;
	}
	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	// step 1: handle struct-level validity and initialize results
	auto left_validity = left.Validity(sel_count);
	auto right_validity = right.Validity(sel_count);
	bool has_nulls = left_validity.CanHaveNull() || right_validity.CanHaveNull();

	// remaining tracks which rows still need child comparison
	// along with their corresponding lhs/rhs selection indices
	SelectionVector remaining_lhs_sel(sel_count);
	SelectionVector remaining_rhs_sel(sel_count);
	SelectionVector remaining_result_sel(sel_count);
	idx_t remaining_count;
	if (!has_nulls) {
		remaining_count = sel_count;
		memset(result_data, 0, sel_count * sizeof(int8_t));
		for (idx_t i = 0; i < sel_count; i++) {
			remaining_lhs_sel.set_index(i, lhs_sel.get_index(i));
			remaining_rhs_sel.set_index(i, rhs_sel.get_index(i));
			remaining_result_sel.set_index(i, i);
		}
	} else {
		remaining_count = 0;
		for (idx_t i = 0; i < sel_count; i++) {
			bool left_null = !left_validity.IsValid(lhs_sel.get_index(i));
			bool right_null = !right_validity.IsValid(rhs_sel.get_index(i));
			if (left_null || right_null) {
				if (!result_validity) {
					// DISTINCT
					result_data[i] = DistinctNullComparator(left_null, right_null);
				} else {
					// regular comparison - set NULL if any value is NULL
					result_validity->SetInvalid(i);
				}
			} else {
				result_data[i] = Comparator::VALUES_ARE_EQUAL;
				remaining_lhs_sel.set_index(remaining_count, lhs_sel.get_index(i));
				remaining_rhs_sel.set_index(remaining_count, rhs_sel.get_index(i));
				remaining_result_sel.set_index(remaining_count, i);
				remaining_count++;
			}
		}
	}

	// step 2: compare child vectors one by one
	// child results are written densely, then scattered back to the correct output positions
	auto child_result = make_unsafe_uniq_array<int8_t>(remaining_count);
	for (idx_t child_idx = 0; child_idx < lchildren.size() && remaining_count > 0; child_idx++) {
		DistinctComparatorTypeSwitch(lchildren[child_idx], rchildren[child_idx], child_result.get(), remaining_lhs_sel,
		                             remaining_rhs_sel, remaining_count);

		idx_t new_remaining_count = 0;
		for (idx_t i = 0; i < remaining_count; i++) {
			if (child_result[i] != Comparator::VALUES_ARE_EQUAL) {
				// not equal at this position - we found the final result for this row
				result_data[remaining_result_sel.get_index(i)] = child_result[i];
			} else {
				// still equal at this position - need to check the next entry
				remaining_lhs_sel.set_index(new_remaining_count, remaining_lhs_sel.get_index(i));
				remaining_rhs_sel.set_index(new_remaining_count, remaining_rhs_sel.get_index(i));
				remaining_result_sel.set_index(new_remaining_count, remaining_result_sel.get_index(i));
				new_remaining_count++;
			}
		}
		remaining_count = new_remaining_count;
	}
}

struct ListEntryAccessor {
	static Vector &GetChild(Vector &vector) {
		return ListVector::GetEntry(vector);
	}
	static void FlattenChild(Vector &vector) {
		auto &child = ListVector::GetEntry(vector);
		child.Flatten(ListVector::GetListSize(vector));
	}
	static idx_t GetOffset(UnifiedVectorFormat &format, idx_t sel_idx) {
		auto entries = UnifiedVectorFormat::GetData<list_entry_t>(format);
		auto idx = format.sel->get_index(sel_idx);
		return entries[idx].offset;
	}
	static idx_t GetLength(UnifiedVectorFormat &format, idx_t sel_idx) {
		auto entries = UnifiedVectorFormat::GetData<list_entry_t>(format);
		auto idx = format.sel->get_index(sel_idx);
		return entries[idx].length;
	}
};

struct ArrayEntryAccessor {
	explicit ArrayEntryAccessor(idx_t array_size) : array_size(array_size) {
	}
	Vector &GetChild(Vector &vector) {
		return ArrayVector::GetEntry(vector);
	}
	void FlattenChild(Vector &vector) {
		auto &child = ArrayVector::GetEntry(vector);
		child.Flatten(ArrayVector::GetTotalSize(vector));
	}
	idx_t GetOffset(UnifiedVectorFormat &format, idx_t sel_idx) {
		return format.sel->get_index(sel_idx) * array_size;
	}
	idx_t GetLength(UnifiedVectorFormat &, idx_t) {
		return array_size;
	}
	idx_t array_size;
};

template <class ACCESSOR>
static void ListOrArrayComparator(Vector &left, Vector &right, int8_t *result_data, const SelectionVector &lhs_sel,
                                  const SelectionVector &rhs_sel, idx_t sel_count, ACCESSOR accessor,
                                  optional_ptr<ValidityMask> result_validity = nullptr) {
	if (sel_count == 0) {
		return;
	}
	// recursively flatten child vectors so they can be indexed directly via selection vectors
	accessor.FlattenChild(left);
	accessor.FlattenChild(right);
	// step 1: handle top-level validity
	auto left_validity = left.Validity(sel_count);
	auto right_validity = right.Validity(sel_count);
	bool has_nulls = left_validity.CanHaveNull() || right_validity.CanHaveNull();

	SelectionVector remaining_lhs_sel(sel_count);
	SelectionVector remaining_rhs_sel(sel_count);
	SelectionVector remaining_result_sel(sel_count);
	idx_t remaining_count;
	if (!has_nulls) {
		remaining_count = sel_count;
		memset(result_data, 0, sel_count * sizeof(int8_t));
		for (idx_t i = 0; i < sel_count; i++) {
			remaining_lhs_sel.set_index(i, lhs_sel.get_index(i));
			remaining_rhs_sel.set_index(i, rhs_sel.get_index(i));
			remaining_result_sel.set_index(i, i);
		}
	} else {
		remaining_count = 0;
		for (idx_t i = 0; i < sel_count; i++) {
			bool left_null = !left_validity.IsValid(lhs_sel.get_index(i));
			bool right_null = !right_validity.IsValid(rhs_sel.get_index(i));
			if (left_null || right_null) {
				if (!result_validity) {
					// DISTINCT
					result_data[i] = DistinctNullComparator(left_null, right_null);
				} else {
					// regular comparison - set NULL if any value is NULL
					result_validity->SetInvalid(i);
				}
			} else {
				result_data[i] = Comparator::VALUES_ARE_EQUAL;
				remaining_lhs_sel.set_index(remaining_count, lhs_sel.get_index(i));
				remaining_rhs_sel.set_index(remaining_count, rhs_sel.get_index(i));
				remaining_result_sel.set_index(remaining_count, i);
				remaining_count++;
			}
		}
	}
	if (remaining_count == 0) {
		return;
	}

	// step 2: get entries and child vector
	UnifiedVectorFormat left_format, right_format;
	left.ToUnifiedFormat(sel_count, left_format);
	right.ToUnifiedFormat(sel_count, right_format);
	auto &left_child = accessor.GetChild(left);
	auto &right_child = accessor.GetChild(right);

	// step 3: iterate position-by-position through child elements of the list / array
	SelectionVector left_child_sel(remaining_count);
	SelectionVector right_child_sel(remaining_count);
	auto child_result = make_unsafe_uniq_array<int8_t>(remaining_count);

	for (idx_t index_in_list = 0; remaining_count > 0; index_in_list++) {
		// partition remaining into: exhausted (one or both ended) vs active (both have element at pos)
		idx_t active_count = 0;
		for (idx_t i = 0; i < remaining_count; i++) {
			auto left_length = accessor.GetLength(left_format, remaining_lhs_sel.get_index(i));
			auto right_length = accessor.GetLength(right_format, remaining_rhs_sel.get_index(i));
			bool left_exhausted = index_in_list >= left_length;
			bool right_exhausted = index_in_list >= right_length;
			if (left_exhausted || right_exhausted) {
				// either (or both) lists are exhausted at this position
				if (!left_exhausted || !right_exhausted) {
					// one of the lists is shorter than the other - the shorter list is the smallest
					result_data[remaining_result_sel.get_index(i)] =
					    left_exhausted ? Comparator::RIGHT_IS_GREATER : Comparator::LEFT_IS_GREATER;
				}
				// else: same length, all elements matched - result stays 0
			} else {
				auto left_offset = accessor.GetOffset(left_format, remaining_lhs_sel.get_index(i));
				auto right_offset = accessor.GetOffset(right_format, remaining_rhs_sel.get_index(i));
				left_child_sel.set_index(active_count, left_offset + index_in_list);
				right_child_sel.set_index(active_count, right_offset + index_in_list);
				remaining_lhs_sel.set_index(active_count, remaining_lhs_sel.get_index(i));
				remaining_rhs_sel.set_index(active_count, remaining_rhs_sel.get_index(i));
				remaining_result_sel.set_index(active_count, remaining_result_sel.get_index(i));
				active_count++;
			}
		}
		if (active_count == 0) {
			break;
		}

		// compare child elements at this position
		DistinctComparatorTypeSwitch(left_child, right_child, child_result.get(), left_child_sel, right_child_sel,
		                             active_count);

		// partition active into resolved vs still-remaining
		idx_t new_remaining_count = 0;
		for (idx_t i = 0; i < active_count; i++) {
			if (child_result[i] != 0) {
				result_data[remaining_result_sel.get_index(i)] = child_result[i];
			} else {
				remaining_lhs_sel.set_index(new_remaining_count, remaining_lhs_sel.get_index(i));
				remaining_rhs_sel.set_index(new_remaining_count, remaining_rhs_sel.get_index(i));
				remaining_result_sel.set_index(new_remaining_count, remaining_result_sel.get_index(i));
				new_remaining_count++;
			}
		}
		remaining_count = new_remaining_count;
	}
}

static void ListComparator(Vector &left, Vector &right, int8_t *result_data, const SelectionVector &lhs_sel,
                           const SelectionVector &rhs_sel, idx_t sel_count,
                           optional_ptr<ValidityMask> result_validity = nullptr) {
	ListEntryAccessor accessor;
	ListOrArrayComparator(left, right, result_data, lhs_sel, rhs_sel, sel_count, accessor, result_validity);
}

static void ArrayComparator(Vector &left, Vector &right, int8_t *result_data, const SelectionVector &lhs_sel,
                            const SelectionVector &rhs_sel, idx_t sel_count,
                            optional_ptr<ValidityMask> result_validity = nullptr) {
	ArrayEntryAccessor accessor(ArrayType::GetSize(left.GetType()));
	ListOrArrayComparator(left, right, result_data, lhs_sel, rhs_sel, sel_count, accessor, result_validity);
}

static void VariantComparator(Vector &left, Vector &right, int8_t *result_data, const SelectionVector &lhs_sel,
                              const SelectionVector &rhs_sel, idx_t sel_count,
                              optional_ptr<ValidityMask> result_validity = nullptr) {
	RecursiveUnifiedVectorFormat left_recursive_data, right_recursive_data;
	Vector::RecursiveToUnifiedFormat(left, sel_count, left_recursive_data);
	Vector::RecursiveToUnifiedFormat(right, sel_count, right_recursive_data);

	UnifiedVariantVectorData left_variant(left_recursive_data);
	UnifiedVariantVectorData right_variant(right_recursive_data);

	auto &left_data = left_recursive_data.unified;
	auto &right_data = right_recursive_data.unified;
	for (idx_t i = 0; i < sel_count; i++) {
		auto left_idx = left_data.sel->get_index(lhs_sel.get_index(i));
		auto right_idx = right_data.sel->get_index(rhs_sel.get_index(i));

		bool left_null = !left_data.validity.RowIsValid(left_idx);
		bool right_null = !right_data.validity.RowIsValid(right_idx);

		if (left_null || right_null) {
			if (!result_validity) {
				// DISTINCT
				result_data[i] = DistinctNullComparator(left_null, right_null);
			} else {
				// regular comparison - set NULL if any value is NULL
				result_validity->SetInvalid(i);
			}
			continue;
		}

		// both non-NULL: convert to Values and compare
		auto left_val = VariantUtils::ConvertVariantToValue(left_variant, lhs_sel.get_index(i), 0);
		auto right_val = VariantUtils::ConvertVariantToValue(right_variant, rhs_sel.get_index(i), 0);

		LogicalType max_logical_type;
		if (!LogicalType::TryGetMaxLogicalTypeUnchecked(left_val.type(), right_val.type(), max_logical_type)) {
			throw InvalidInputException(
			    "Can't compare values of type %s (%s) and type %s (%s) - an explicit cast is required",
			    left_val.type().ToString(), left_val.ToString(), right_val.type().ToString(), right_val.ToString());
		}

		if (ValueOperations::DistinctGreaterThan(left_val, right_val)) {
			result_data[i] = Comparator::LEFT_IS_GREATER;
		} else if (ValueOperations::DistinctGreaterThan(right_val, left_val)) {
			result_data[i] = Comparator::RIGHT_IS_GREATER;
		} else {
			result_data[i] = Comparator::VALUES_ARE_EQUAL;
		}
	}
}

static void DistinctComparatorTypeSwitchInternal(Vector &left, Vector &right, int8_t *result_data,
                                                 const SelectionVector &lhs_sel, const SelectionVector &rhs_sel,
                                                 idx_t sel_count) {
	D_ASSERT(left.GetType().InternalType() == right.GetType().InternalType());
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		DistinctComparatorExecute::Execute<int8_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::INT16:
		DistinctComparatorExecute::Execute<int16_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::INT32:
		DistinctComparatorExecute::Execute<int32_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::INT64:
		DistinctComparatorExecute::Execute<int64_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::UINT8:
		DistinctComparatorExecute::Execute<uint8_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::UINT16:
		DistinctComparatorExecute::Execute<uint16_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::UINT32:
		DistinctComparatorExecute::Execute<uint32_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::UINT64:
		DistinctComparatorExecute::Execute<uint64_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::INT128:
		DistinctComparatorExecute::Execute<hugeint_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::UINT128:
		DistinctComparatorExecute::Execute<uhugeint_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::FLOAT:
		DistinctComparatorExecute::Execute<float>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::DOUBLE:
		DistinctComparatorExecute::Execute<double>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::INTERVAL:
		DistinctComparatorExecute::Execute<interval_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::VARCHAR:
		DistinctComparatorExecute::Execute<string_t>(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::STRUCT:
		if (left.GetType().id() == LogicalTypeId::VARIANT) {
			VariantComparator(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		} else {
			StructComparator(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		}
		break;
	case PhysicalType::LIST:
		ListComparator(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	case PhysicalType::ARRAY:
		ArrayComparator(left, right, result_data, lhs_sel, rhs_sel, sel_count);
		break;
	default:
		throw InternalException("Invalid type for comparator");
	}
}

static void DistinctComparatorTypeSwitch(Vector &left, Vector &right, int8_t *result_data,
                                         const SelectionVector &lhs_sel, const SelectionVector &rhs_sel,
                                         idx_t sel_count) {
	DistinctComparatorTypeSwitchInternal(left, right, result_data, lhs_sel, rhs_sel, sel_count);
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
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY: {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<int8_t>(result);
		auto &validity = FlatVector::Validity(result);
		auto &sel = *FlatVector::IncrementalSelectionVector();
		auto physical_type = left.GetType().InternalType();
		if (physical_type == PhysicalType::STRUCT && left.GetType().id() == LogicalTypeId::VARIANT) {
			VariantComparator(left, right, result_data, sel, sel, count, validity);
		} else if (physical_type == PhysicalType::STRUCT) {
			StructComparator(left, right, result_data, sel, sel, count, validity);
		} else if (physical_type == PhysicalType::LIST) {
			ListComparator(left, right, result_data, sel, sel, count, validity);
		} else {
			ArrayComparator(left, right, result_data, sel, sel, count, validity);
		}
		break;
	}
	default:
		throw InternalException("Invalid type for comparator");
	}
}

void VectorOperations::Comparator(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparatorTypeSwitch(left, right, result, count);
}

template <class T, class OP>
static void DistinctExecuteGenericLoop(const T *__restrict ldata, const T *__restrict rdata,
                                       int8_t *__restrict result_data, const SelectionVector *__restrict lsel,
                                       const SelectionVector *__restrict rsel, idx_t count, ValidityMask &lmask,
                                       ValidityMask &rmask) {
	for (idx_t i = 0; i < count; i++) {
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		result_data[i] = OP::template Operation<T>(ldata[lindex], rdata[rindex], !lmask.RowIsValid(lindex),
		                                           !rmask.RowIsValid(rindex));
	}
}

template <class T, class OP>
static void DistinctExecuteConstant(Vector &left, Vector &right, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto ldata = ConstantVector::GetData<T>(left);
	auto rdata = ConstantVector::GetData<T>(right);
	auto result_data = ConstantVector::GetData<int8_t>(result);
	*result_data =
	    OP::template Operation<T>(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right));
}

template <class T, class OP>
static void DistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		DistinctExecuteConstant<T, OP>(left, right, result);
	} else {
		UnifiedVectorFormat ldata, rdata;
		left.ToUnifiedFormat(count, ldata);
		right.ToUnifiedFormat(count, rdata);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<int8_t>(result);
		DistinctExecuteGenericLoop<T, OP>(UnifiedVectorFormat::GetData<T>(ldata),
		                                  UnifiedVectorFormat::GetData<T>(rdata), result_data, ldata.sel, rdata.sel,
		                                  count, ldata.validity, rdata.validity);
	}
}

template <class OP>
static bool TryPrimitiveDistinctComparatorExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
#ifdef DUCKDB_SMALLER_BINARY
	return false;
#else
	D_ASSERT(left.GetType().InternalType() == right.GetType().InternalType());
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		DistinctExecute<int8_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT16:
		DistinctExecute<int16_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT32:
		DistinctExecute<int32_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT64:
		DistinctExecute<int64_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT8:
		DistinctExecute<uint8_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT16:
		DistinctExecute<uint16_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT32:
		DistinctExecute<uint32_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT64:
		DistinctExecute<uint64_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::INT128:
		DistinctExecute<hugeint_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::UINT128:
		DistinctExecute<uhugeint_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::FLOAT:
		DistinctExecute<float, OP>(left, right, result, count);
		return true;
	case PhysicalType::DOUBLE:
		DistinctExecute<double, OP>(left, right, result, count);
		return true;
	case PhysicalType::INTERVAL:
		DistinctExecute<interval_t, OP>(left, right, result, count);
		return true;
	case PhysicalType::VARCHAR:
		DistinctExecute<string_t, OP>(left, right, result, count);
		return true;
	default:
		return false;
	}
#endif
}

void VectorOperations::DistinctComparator(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::TINYINT);
	if (TryPrimitiveDistinctComparatorExecute<duckdb::DistinctComparator>(left, right, result, count)) {
		return;
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<int8_t>(result);
	auto &sel = *FlatVector::IncrementalSelectionVector();
	DistinctComparatorTypeSwitchInternal(left, right, result_data, sel, sel, count);
}

void VectorOperations::DistinctComparatorNullsFirst(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (TryPrimitiveDistinctComparatorExecute<duckdb::DistinctComparatorNullsFirst>(left, right, result, count)) {
		return;
	}
	// run the NULLS LAST comparator, then flip the sign for NULL-involving rows
	// note that even for NULLS FIRST, ONLY the top-level is NULLS FIRST,
	// i.e. within structs we still use NULLS LAST semantics
	VectorOperations::DistinctComparator(left, right, result, count);
	result.Flatten(count);
	auto result_data = FlatVector::GetData<int8_t>(result);
	auto left_validity = left.Validity(count);
	auto right_validity = right.Validity(count);
	if (!left_validity.CanHaveNull() && !right_validity.CanHaveNull()) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		bool left_null = !left_validity.IsValid(i);
		bool right_null = !right_validity.IsValid(i);
		if ((left_null || right_null) && !(left_null && right_null)) {
			result_data[i] = UnsafeNumericCast<int8_t>(-result_data[i]);
		}
	}
}

} // namespace duckdb
