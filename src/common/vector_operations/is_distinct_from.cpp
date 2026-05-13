#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

void VectorOperations::DistinctFrom(const Vector &left, const Vector &right, Vector &result) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);
	Vector comparator_result(LogicalType::TINYINT);
	VectorOperations::DistinctComparator(left, right, comparator_result);
	const idx_t count = comparator_result.size();
	auto cmp_data = comparator_result.Values<int8_t>();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::Writer<bool>(result, count);
	for (idx_t i = 0; i < count; i++) {
		result_data.WriteValue(cmp_data[i].GetValueUnsafe() != 0);
	}
	FlatVector::SetSize(result, count);
}

void VectorOperations::NotDistinctFrom(const Vector &left, const Vector &right, Vector &result) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);
	Vector comparator_result(LogicalType::TINYINT);
	VectorOperations::DistinctComparator(left, right, comparator_result);
	const idx_t count = comparator_result.size();
	auto cmp_data = comparator_result.Values<int8_t>();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::Writer<bool>(result, count);
	for (idx_t i = 0; i < count; i++) {
		result_data.WriteValue(cmp_data[i].GetValueUnsafe() == 0);
	}
	FlatVector::SetSize(result, count);
}

template <class FILL_FN, class PREDICATE>
static idx_t DistinctComparatorSelect(const Vector &left, const Vector &right,
                                      optional_ptr<const SelectionVector> sel, idx_t count,
                                      optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                      FILL_FN fill_fn, PREDICATE predicate) {
	Vector comparator_result(LogicalType::TINYINT, count);
	fill_fn(left, right, comparator_result, count);
	auto cmp_data = comparator_result.Values<int8_t>();

	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = sel ? sel->get_index(i) : i;
		if (predicate(cmp_data[i].GetValueUnsafe())) {
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

// true := A != B with nulls being equal
idx_t VectorOperations::DistinctFrom(const Vector &left, const Vector &right,
                                     optional_ptr<const SelectionVector> sel, idx_t count,
                                     optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v != 0; });
}
// true := A == B with nulls being equal
idx_t VectorOperations::NotDistinctFrom(const Vector &left, const Vector &right,
                                        optional_ptr<const SelectionVector> sel, idx_t count,
                                        optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v == 0; });
}

// true := A > B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThan(const Vector &left, const Vector &right,
                                            optional_ptr<const SelectionVector> sel, idx_t count,
                                            optional_ptr<SelectionVector> true_sel,
                                            optional_ptr<SelectionVector> false_sel,
                                            optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v > 0; });
}

// true := A > B with nulls being minimal
idx_t VectorOperations::DistinctGreaterThanNullsFirst(const Vector &left, const Vector &right,
                                                      optional_ptr<const SelectionVector> sel, idx_t count,
                                                      optional_ptr<SelectionVector> true_sel,
                                                      optional_ptr<SelectionVector> false_sel,
                                                      optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel,
	                                VectorOperations::DistinctComparatorNullsFirstFill, [](int8_t v) { return v > 0; });
}

// true := A >= B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThanEquals(const Vector &left, const Vector &right,
                                                  optional_ptr<const SelectionVector> sel, idx_t count,
                                                  optional_ptr<SelectionVector> true_sel,
                                                  optional_ptr<SelectionVector> false_sel,
                                                  optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v >= 0; });
}
// true := A < B with nulls being maximal
idx_t VectorOperations::DistinctLessThan(const Vector &left, const Vector &right,
                                         optional_ptr<const SelectionVector> sel, idx_t count,
                                         optional_ptr<SelectionVector> true_sel,
                                         optional_ptr<SelectionVector> false_sel,
                                         optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v < 0; });
}

// true := A < B with nulls being minimal
idx_t VectorOperations::DistinctLessThanNullsFirst(const Vector &left, const Vector &right,
                                                   optional_ptr<const SelectionVector> sel, idx_t count,
                                                   optional_ptr<SelectionVector> true_sel,
                                                   optional_ptr<SelectionVector> false_sel,
                                                   optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel,
	                                VectorOperations::DistinctComparatorNullsFirstFill, [](int8_t v) { return v < 0; });
}

// true := A <= B with nulls being maximal
idx_t VectorOperations::DistinctLessThanEquals(const Vector &left, const Vector &right,
                                               optional_ptr<const SelectionVector> sel, idx_t count,
                                               optional_ptr<SelectionVector> true_sel,
                                               optional_ptr<SelectionVector> false_sel,
                                               optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v <= 0; });
}

// true := A != B with nulls being equal, inputs selected
idx_t VectorOperations::NestedNotEquals(const Vector &left, const Vector &right,
                                        optional_ptr<const SelectionVector> sel, idx_t count,
                                        optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v != 0; });
}
// true := A == B with nulls being equal, inputs selected
idx_t VectorOperations::NestedEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                     idx_t count, optional_ptr<SelectionVector> true_sel,
                                     optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	return DistinctComparatorSelect(left, right, sel, count, true_sel, false_sel, VectorOperations::DistinctComparatorFill,
	                                [](int8_t v) { return v == 0; });
}

} // namespace duckdb
