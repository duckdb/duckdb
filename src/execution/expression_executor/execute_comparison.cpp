#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"

namespace duckdb {

enum class StructEqualityResult : uint8_t { FALSE_VALUE, TRUE_VALUE, NULL_VALUE };

static StructEqualityResult EvaluateStructEquality(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		return StructEqualityResult::NULL_VALUE;
	}
	auto &left_children = StructValue::GetChildren(left);
	auto &right_children = StructValue::GetChildren(right);
	D_ASSERT(left_children.size() == right_children.size());
	bool has_unknown = false;
	for (idx_t i = 0; i < left_children.size(); i++) {
		auto &lhs_child = left_children[i];
		auto &rhs_child = right_children[i];
		if (lhs_child.type().InternalType() == PhysicalType::STRUCT &&
		    rhs_child.type().InternalType() == PhysicalType::STRUCT) {
			auto child_result = EvaluateStructEquality(lhs_child, rhs_child);
			if (child_result == StructEqualityResult::FALSE_VALUE) {
				return StructEqualityResult::FALSE_VALUE;
			}
			has_unknown = has_unknown || child_result == StructEqualityResult::NULL_VALUE;
			continue;
		}
		if (lhs_child.IsNull() || rhs_child.IsNull()) {
			has_unknown = true;
			continue;
		}
		if (!ValueOperations::NotDistinctFrom(lhs_child, rhs_child)) {
			return StructEqualityResult::FALSE_VALUE;
		}
	}
	return has_unknown ? StructEqualityResult::NULL_VALUE : StructEqualityResult::TRUE_VALUE;
}

static idx_t SelectStructEqualsOrNotEquals(const Vector &left, const Vector &right,
                                           optional_ptr<const SelectionVector> sel, idx_t count,
                                           optional_ptr<SelectionVector> true_sel,
                                           optional_ptr<SelectionVector> false_sel,
                                           optional_ptr<ValidityMask> null_mask, bool invert) {
	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = sel ? sel->get_index(i) : i;
		auto row_result = EvaluateStructEquality(left.GetValue(result_idx), right.GetValue(result_idx));
		if (row_result == StructEqualityResult::NULL_VALUE) {
			if (null_mask) {
				null_mask->SetInvalid(result_idx);
			}
			if (false_sel) {
				false_sel->set_index(false_count++, result_idx);
			}
			continue;
		}
		const bool matches = row_result == StructEqualityResult::TRUE_VALUE ? !invert : invert;
		if (matches) {
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

//===--------------------------------------------------------------------===//
// Select comparisons
//===--------------------------------------------------------------------===//
template <class OP>
static bool TryPrimitiveSelectOperation(const Vector &left, const Vector &right,
                                        optional_ptr<const SelectionVector> sel, idx_t count,
                                        optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                        optional_ptr<ValidityMask> null_mask, idx_t &result) {
#ifdef DUCKDB_SMALLER_BINARY
	return false;
#else
	if (null_mask) {
		auto left_validity = left.Validity();
		auto right_validity = right.Validity();
		if (left_validity.CanHaveNull() || right_validity.CanHaveNull()) {
			if (!sel) {
				sel = FlatVector::IncrementalSelectionVector();
			}
			for (idx_t i = 0; i < count; ++i) {
				if (!left_validity.IsValid(i) || !right_validity.IsValid(i)) {
					null_mask->SetInvalid(sel->get_index(i));
				}
			}
		}
	}
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		result =
		    BinaryExecutor::Select<int8_t, int8_t, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get());
		return true;
	case PhysicalType::INT16:
		result = BinaryExecutor::Select<int16_t, int16_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
		return true;
	case PhysicalType::INT32:
		result = BinaryExecutor::Select<int32_t, int32_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
		return true;
	case PhysicalType::INT64:
		result = BinaryExecutor::Select<int64_t, int64_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
		return true;
	case PhysicalType::UINT8:
		result = BinaryExecutor::Select<uint8_t, uint8_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
		return true;
	case PhysicalType::UINT16:
		result = BinaryExecutor::Select<uint16_t, uint16_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                        false_sel.get());
		return true;
	case PhysicalType::UINT32:
		result = BinaryExecutor::Select<uint32_t, uint32_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                        false_sel.get());
		return true;
	case PhysicalType::UINT64:
		result = BinaryExecutor::Select<uint64_t, uint64_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                        false_sel.get());
		return true;
	case PhysicalType::INT128:
		result = BinaryExecutor::Select<hugeint_t, hugeint_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                          false_sel.get());
		return true;
	case PhysicalType::UINT128:
		result = BinaryExecutor::Select<uhugeint_t, uhugeint_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                            false_sel.get());
		return true;
	case PhysicalType::FLOAT:
		result =
		    BinaryExecutor::Select<float, float, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get());
		return true;
	case PhysicalType::DOUBLE:
		result =
		    BinaryExecutor::Select<double, double, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get());
		return true;
	case PhysicalType::INTERVAL:
		result = BinaryExecutor::Select<interval_t, interval_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                            false_sel.get());
		return true;
	case PhysicalType::VARCHAR:
		result = BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                        false_sel.get());
		return true;
	default:
		return false;
	}
#endif
}

template <class PREDICATE>
static idx_t ComparatorSelectOperation(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                       idx_t count, optional_ptr<SelectionVector> true_sel,
                                       optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask,
                                       PREDICATE predicate) {
	Vector comparator_result(LogicalType::TINYINT, count);
	VectorOperations::ComparatorFill(left, right, comparator_result, count);
	auto cmp_data = comparator_result.Values<int8_t>();

	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = sel->get_index(i);
		auto entry = cmp_data[i];
		if (!entry.IsValid()) {
			// NULL result: goes to false_sel, mark in null_mask
			if (null_mask) {
				null_mask->SetInvalid(result_idx);
			}
			if (false_sel) {
				false_sel->set_index(false_count, result_idx);
			}
			false_count++;
		} else if (predicate(entry.GetValue())) {
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

idx_t VectorOperations::Equals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                               idx_t count, optional_ptr<SelectionVector> true_sel,
                               optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	if (left.GetType().InternalType() == PhysicalType::STRUCT &&
	    right.GetType().InternalType() == PhysicalType::STRUCT) {
		return SelectStructEqualsOrNotEquals(left, right, sel, count, true_sel, false_sel, null_mask, false);
	}
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel, null_mask, result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v == Comparator::VALUES_ARE_EQUAL; });
}

idx_t VectorOperations::NotEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                  idx_t count, optional_ptr<SelectionVector> true_sel,
                                  optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	if (left.GetType().InternalType() == PhysicalType::STRUCT &&
	    right.GetType().InternalType() == PhysicalType::STRUCT) {
		return SelectStructEqualsOrNotEquals(left, right, sel, count, true_sel, false_sel, null_mask, true);
	}
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel, null_mask,
	                                                   result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v != Comparator::VALUES_ARE_EQUAL; });
}

idx_t VectorOperations::GreaterThan(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                    idx_t count, optional_ptr<SelectionVector> true_sel,
                                    optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel, null_mask,
	                                                     result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v > 0; });
}

idx_t VectorOperations::GreaterThanEquals(const Vector &left, const Vector &right,
                                          optional_ptr<const SelectionVector> sel, idx_t count,
                                          optional_ptr<SelectionVector> true_sel,
                                          optional_ptr<SelectionVector> false_sel,
                                          optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel, null_mask,
	                                                           result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v >= 0; });
}

idx_t VectorOperations::LessThan(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                 idx_t count, optional_ptr<SelectionVector> true_sel,
                                 optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::GreaterThan>(right, left, sel, count, true_sel, false_sel, null_mask,
	                                                     result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v < 0; });
}

idx_t VectorOperations::LessThanEquals(const Vector &left, const Vector &right, optional_ptr<const SelectionVector> sel,
                                       idx_t count, optional_ptr<SelectionVector> true_sel,
                                       optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::GreaterThanEquals>(right, left, sel, count, true_sel, false_sel, null_mask,
	                                                           result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v <= 0; });
}

} // namespace duckdb
