#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundComparisonExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->AddChild(*expr.left);
	result->AddChild(*expr.right);

	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// resolve the children
	state->intermediate_chunk.Reset();
	auto &left = state->intermediate_chunk.data[0];
	auto &right = state->intermediate_chunk.data[1];

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	bool all_constant = false;
	if (state->intermediate_chunk.AllConstant()) {
		count = 1;
		all_constant = true;
	}
	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		VectorOperations::DistinctFrom(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		VectorOperations::NotDistinctFrom(left, right, result, count);
		break;
	default:
		throw InternalException("Unknown comparison type!");
	}
	if (all_constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//===--------------------------------------------------------------------===//
// Select comparisons
//===--------------------------------------------------------------------===//
template <class OP>
static bool TryPrimitiveSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                        idx_t count, optional_ptr<SelectionVector> true_sel,
                                        optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask,
                                        idx_t &result) {
#ifdef DUCKDB_SMALLER_BINARY
	return false;
#else
	if (null_mask) {
		auto left_validity = left.Validity(count);
		auto right_validity = right.Validity(count);
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
static idx_t ComparatorSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                       idx_t count, optional_ptr<SelectionVector> true_sel,
                                       optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask,
                                       PREDICATE predicate) {
	Vector comparator_result(LogicalType::TINYINT, count);
	VectorOperations::Comparator(left, right, comparator_result, count);
	auto cmp_data = comparator_result.Values<int8_t>(count);

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

idx_t VectorOperations::Equals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                               optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                               optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel, null_mask, result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v == Comparator::VALUES_ARE_EQUAL; });
}

idx_t VectorOperations::NotEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                  optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                  optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel, null_mask,
	                                                   result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v != Comparator::VALUES_ARE_EQUAL; });
}

idx_t VectorOperations::GreaterThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                    optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel, null_mask,
	                                                     result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v > 0; });
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                          idx_t count, optional_ptr<SelectionVector> true_sel,
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

idx_t VectorOperations::LessThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                 optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                 optional_ptr<ValidityMask> null_mask) {
	idx_t result;
	if (TryPrimitiveSelectOperation<duckdb::GreaterThan>(right, left, sel, count, true_sel, false_sel, null_mask,
	                                                     result)) {
		return result;
	}
	return ComparatorSelectOperation(left, right, sel, count, true_sel, false_sel, null_mask,
	                                 [](int8_t v) { return v < 0; });
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
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

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	// resolve the children
	state->intermediate_chunk.Reset();
	auto &left = state->intermediate_chunk.data[0];
	auto &right = state->intermediate_chunk.data[1];

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.GetExpressionType()) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Unknown comparison type!");
	}
}

} // namespace duckdb
