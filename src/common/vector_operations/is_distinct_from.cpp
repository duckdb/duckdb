#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

void VectorOperations::DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);
	Vector comparator_result(LogicalType::TINYINT, count);
	VectorOperations::DistinctComparator(left, right, comparator_result, count);
	auto cmp_data = FlatVector::GetData<int8_t>(comparator_result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = cmp_data[i] != 0;
	}
}

void VectorOperations::NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);
	Vector comparator_result(LogicalType::TINYINT, count);
	VectorOperations::DistinctComparator(left, right, comparator_result, count);
	auto cmp_data = FlatVector::GetData<int8_t>(comparator_result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = cmp_data[i] == 0;
	}
}

template <class COMPARATOR_FN, class PREDICATE>
static idx_t DistinctComparatorSelect(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                      optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                      COMPARATOR_FN comparator_fn, PREDICATE predicate) {
	Vector comparator_result(LogicalType::TINYINT, count);
	comparator_fn(left, right, comparator_result, count);
	auto cmp_data = FlatVector::GetData<int8_t>(comparator_result);

	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = sel ? sel->get_index(i) : i;
		if (predicate(cmp_data[i])) {
			if (true_sel) {
				true_sel->set_index(true_count, result_idx);
			}
			true_count++;
		} else {
			if (false_sel) {
				false_sel->set_index(false_count, result_idx);
			}
			false_count++;
		}
	}
	return true_count;
}

static auto DistinctComparatorFn = VectorOperations::DistinctComparator;
static auto DistinctComparatorNullsFirstFn = VectorOperations::DistinctComparatorNullsFirst;

// true := A != B with nulls being equal
idx_t VectorOperations::DistinctFrom(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                     optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v != 0; });
}
// true := A == B with nulls being equal
idx_t VectorOperations::NotDistinctFrom(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                        idx_t count, optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v == 0; });
}

// true := A > B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                            idx_t count, optional_ptr<SelectionVector> true_sel,
                                            optional_ptr<SelectionVector> false_sel,
                                            optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v > 0; });
}

// true := A > B with nulls being minimal
idx_t VectorOperations::DistinctGreaterThanNullsFirst(Vector &left, Vector &right,
                                                      optional_ptr<const SelectionVector> sel, idx_t count,
                                                      optional_ptr<SelectionVector> true_sel,
                                                      optional_ptr<SelectionVector> false_sel,
                                                      optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorNullsFirstFn,
	                                [](int8_t v) { return v > 0; });
}

// true := A >= B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                                  idx_t count, optional_ptr<SelectionVector> true_sel,
                                                  optional_ptr<SelectionVector> false_sel,
                                                  optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v >= 0; });
}
// true := A < B with nulls being maximal
idx_t VectorOperations::DistinctLessThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                         idx_t count, optional_ptr<SelectionVector> true_sel,
                                         optional_ptr<SelectionVector> false_sel,
                                         optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v < 0; });
}

// true := A < B with nulls being minimal
idx_t VectorOperations::DistinctLessThanNullsFirst(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                                   idx_t count, optional_ptr<SelectionVector> true_sel,
                                                   optional_ptr<SelectionVector> false_sel,
                                                   optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorNullsFirstFn,
	                                [](int8_t v) { return v < 0; });
}

// true := A <= B with nulls being maximal
idx_t VectorOperations::DistinctLessThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                               idx_t count, optional_ptr<SelectionVector> true_sel,
                                               optional_ptr<SelectionVector> false_sel,
                                               optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v <= 0; });
}

// true := A != B with nulls being equal, inputs selected
idx_t VectorOperations::NestedNotEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                        idx_t count, optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v != 0; });
}
// true := A == B with nulls being equal, inputs selected
idx_t VectorOperations::NestedEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                     optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                     optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, DistinctComparatorFn,
	                                [](int8_t v) { return v == 0; });
}

} // namespace duckdb
