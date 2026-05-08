#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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

template <bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t SelectAllFalse(const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
                                   SelectionVector *false_sel) {
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = sel.get_index(i);
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count++, input_idx);
		}
	}
	if (HAS_TRUE_SEL) {
		return 0;
	}
	return count;
}

static inline idx_t SelectAllFalseSwitch(const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
                                         SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return SelectAllFalse<true, true>(sel, count, true_sel, false_sel);
	} else if (true_sel) {
		return SelectAllFalse<true, false>(sel, count, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return SelectAllFalse<false, true>(sel, count, true_sel, false_sel);
	}
}

template <bool SELECT_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t SelectNullLoop(const UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	auto &mask = vdata.validity;
	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = sel.get_index(i);
		// The current selection stores chunk-local row ids. UnifiedVectorFormat::sel resolves those row ids to the
		// physical entry in the referenced vector (e.g. after dictionary slicing).
		auto vector_idx = vdata.sel->get_index(input_idx);
		bool comparison_result = mask.RowIsValid(vector_idx) != SELECT_NULL;
		if (HAS_TRUE_SEL) {
			true_sel->set_index(true_count, input_idx);
			true_count += comparison_result;
		}
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count, input_idx);
			false_count += !comparison_result;
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	}
	return count - false_count;
}

template <bool SELECT_NULL>
static inline idx_t SelectNullLoopSwitch(const UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                                         SelectionVector *true_sel, SelectionVector *false_sel) {
	if (vdata.validity.CannotHaveNull()) {
		if (SELECT_NULL) {
			return SelectAllFalseSwitch(sel, count, true_sel, false_sel);
		}
		if (true_sel) {
			for (idx_t i = 0; i < count; i++) {
				true_sel->set_index(i, sel.get_index(i));
			}
			return count;
		}
		return count;
	}
	if (true_sel && false_sel) {
		return SelectNullLoop<SELECT_NULL, true, true>(vdata, sel, count, true_sel, false_sel);
	} else if (true_sel) {
		return SelectNullLoop<SELECT_NULL, true, false>(vdata, sel, count, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return SelectNullLoop<SELECT_NULL, false, true>(vdata, sel, count, true_sel, false_sel);
	}
}

template <class T, class OP, bool HAS_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t SelectConstantComparisonLoop(const UnifiedVectorFormat &vdata, T constant,
                                                 const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
                                                 SelectionVector *false_sel) {
	const auto data = UnifiedVectorFormat::GetData<const T>(vdata);
	auto &mask = vdata.validity;
	idx_t true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = sel.get_index(i);
		auto vector_idx = vdata.sel->get_index(input_idx);
		bool comparison_result =
		    (!HAS_NULL || mask.RowIsValidUnsafe(vector_idx)) && OP::Operation(data[vector_idx], constant);
		if (HAS_TRUE_SEL) {
			true_sel->set_index(true_count, input_idx);
			true_count += comparison_result;
		}
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count, input_idx);
			false_count += !comparison_result;
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	}
	return count - false_count;
}

template <class T, class OP, bool HAS_NULL>
static inline idx_t SelectConstantComparisonLoopSwitch(const UnifiedVectorFormat &vdata, T constant,
                                                       const SelectionVector &sel, idx_t count,
                                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return SelectConstantComparisonLoop<T, OP, HAS_NULL, true, true>(vdata, constant, sel, count, true_sel,
		                                                                 false_sel);
	} else if (true_sel) {
		return SelectConstantComparisonLoop<T, OP, HAS_NULL, true, false>(vdata, constant, sel, count, true_sel,
		                                                                  false_sel);
	} else {
		D_ASSERT(false_sel);
		return SelectConstantComparisonLoop<T, OP, HAS_NULL, false, true>(vdata, constant, sel, count, true_sel,
		                                                                  false_sel);
	}
}

template <class T>
static inline idx_t SelectConstantComparison(const UnifiedVectorFormat &vdata, T constant,
                                             ExpressionType comparison_type, const SelectionVector &sel, idx_t count,
                                             SelectionVector *true_sel, SelectionVector *false_sel) {
	const bool has_null = vdata.validity.CanHaveNull();
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return has_null ? SelectConstantComparisonLoopSwitch<T, duckdb::Equals, true>(vdata, constant, sel, count,
		                                                                              true_sel, false_sel)
		                : SelectConstantComparisonLoopSwitch<T, duckdb::Equals, false>(vdata, constant, sel, count,
		                                                                               true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return has_null ? SelectConstantComparisonLoopSwitch<T, duckdb::NotEquals, true>(vdata, constant, sel, count,
		                                                                                 true_sel, false_sel)
		                : SelectConstantComparisonLoopSwitch<T, duckdb::NotEquals, false>(vdata, constant, sel, count,
		                                                                                  true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return has_null ? SelectConstantComparisonLoopSwitch<T, duckdb::LessThan, true>(vdata, constant, sel, count,
		                                                                                true_sel, false_sel)
		                : SelectConstantComparisonLoopSwitch<T, duckdb::LessThan, false>(vdata, constant, sel, count,
		                                                                                 true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return has_null ? SelectConstantComparisonLoopSwitch<T, duckdb::GreaterThan, true>(vdata, constant, sel, count,
		                                                                                   true_sel, false_sel)
		                : SelectConstantComparisonLoopSwitch<T, duckdb::GreaterThan, false>(vdata, constant, sel, count,
		                                                                                    true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return has_null
		           ? SelectConstantComparisonLoopSwitch<T, duckdb::LessThanEquals, true>(vdata, constant, sel, count,
		                                                                                 true_sel, false_sel)
		           : SelectConstantComparisonLoopSwitch<T, duckdb::LessThanEquals, false>(vdata, constant, sel, count,
		                                                                                  true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return has_null ? SelectConstantComparisonLoopSwitch<T, duckdb::GreaterThanEquals, true>(
		                      vdata, constant, sel, count, true_sel, false_sel)
		                : SelectConstantComparisonLoopSwitch<T, duckdb::GreaterThanEquals, false>(
		                      vdata, constant, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Unsupported constant comparison type in SelectConstantComparison");
	}
}

struct ReferenceConstantComparison {
	optional_ptr<const BoundReferenceExpression> ref_expr;
	optional_ptr<const BoundConstantExpression> const_expr;
	ExpressionType comparison_type;
};

static bool TryGetReferenceConstantComparison(const BoundComparisonExpression &expr,
                                              ReferenceConstantComparison &comparison) {
	comparison.comparison_type = expr.GetExpressionType();
	if (expr.left->GetExpressionClass() == ExpressionClass::BOUND_REF &&
	    expr.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		comparison.ref_expr = expr.left->Cast<BoundReferenceExpression>();
		comparison.const_expr = expr.right->Cast<BoundConstantExpression>();
		return true;
	}
	if (expr.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	    expr.right->GetExpressionClass() == ExpressionClass::BOUND_REF) {
		comparison.ref_expr = expr.right->Cast<BoundReferenceExpression>();
		comparison.const_expr = expr.left->Cast<BoundConstantExpression>();
		comparison.comparison_type = FlipComparisonExpression(comparison.comparison_type);
		return true;
	}
	return false;
}

static ExpressionType NormalizeConstantComparisonType(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return comparison_type;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		// Once the constant is known to be non-NULL, NOT DISTINCT FROM is the same as =.
		return ExpressionType::COMPARE_EQUAL;
	default:
		return ExpressionType::INVALID;
	}
}

static bool TrySelectReferenceConstantComparison(DataChunk &chunk, const ReferenceConstantComparison &comparison,
                                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                                 SelectionVector *false_sel, idx_t &result_count) {
	if (!comparison.ref_expr || !comparison.const_expr || comparison.ref_expr->index >= chunk.ColumnCount()) {
		return false;
	}
	auto &reference_input = chunk.data[comparison.ref_expr->index];
	const auto &result_sel = sel ? *sel : *FlatVector::IncrementalSelectionVector();
	const auto &constant = comparison.const_expr->value;
	UnifiedVectorFormat vdata;
	// This avoids materializing the reference and constant children into temporary vectors for the common
	// "column <cmp> constant" scan predicate shape.
	reference_input.ToUnifiedFormat(vdata);
	if (constant.IsNull()) {
		switch (comparison.comparison_type) {
		case ExpressionType::COMPARE_DISTINCT_FROM:
			result_count = SelectNullLoopSwitch<false>(vdata, result_sel, count, true_sel, false_sel);
			return true;
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			result_count = SelectNullLoopSwitch<true>(vdata, result_sel, count, true_sel, false_sel);
			return true;
		default:
			result_count = SelectAllFalseSwitch(result_sel, count, true_sel, false_sel);
			return true;
		}
	}

	auto comparison_type = NormalizeConstantComparisonType(comparison.comparison_type);
	if (comparison_type == ExpressionType::INVALID) {
		return false;
	}

	switch (reference_input.GetType().InternalType()) {
	case PhysicalType::BOOL:
		result_count = SelectConstantComparison<bool>(vdata, constant.GetValueUnsafe<bool>(), comparison_type,
		                                              result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::INT8:
		result_count = SelectConstantComparison<int8_t>(vdata, constant.GetValueUnsafe<int8_t>(), comparison_type,
		                                                result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::INT16:
		result_count = SelectConstantComparison<int16_t>(vdata, constant.GetValueUnsafe<int16_t>(), comparison_type,
		                                                 result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::INT32:
		result_count = SelectConstantComparison<int32_t>(vdata, constant.GetValueUnsafe<int32_t>(), comparison_type,
		                                                 result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::INT64:
		result_count = SelectConstantComparison<int64_t>(vdata, constant.GetValueUnsafe<int64_t>(), comparison_type,
		                                                 result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::INT128:
		result_count = SelectConstantComparison<hugeint_t>(vdata, constant.GetValueUnsafe<hugeint_t>(), comparison_type,
		                                                   result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::UINT8:
		result_count = SelectConstantComparison<uint8_t>(vdata, constant.GetValueUnsafe<uint8_t>(), comparison_type,
		                                                 result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::UINT16:
		result_count = SelectConstantComparison<uint16_t>(vdata, constant.GetValueUnsafe<uint16_t>(), comparison_type,
		                                                  result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::UINT32:
		result_count = SelectConstantComparison<uint32_t>(vdata, constant.GetValueUnsafe<uint32_t>(), comparison_type,
		                                                  result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::UINT64:
		result_count = SelectConstantComparison<uint64_t>(vdata, constant.GetValueUnsafe<uint64_t>(), comparison_type,
		                                                  result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::UINT128:
		result_count = SelectConstantComparison<uhugeint_t>(vdata, constant.GetValueUnsafe<uhugeint_t>(),
		                                                    comparison_type, result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::FLOAT:
		result_count = SelectConstantComparison<float>(vdata, constant.GetValueUnsafe<float>(), comparison_type,
		                                               result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::DOUBLE:
		result_count = SelectConstantComparison<double>(vdata, constant.GetValueUnsafe<double>(), comparison_type,
		                                                result_sel, count, true_sel, false_sel);
		return true;
	case PhysicalType::VARCHAR:
		result_count = SelectConstantComparison<string_t>(vdata, constant.GetValueUnsafe<string_t>(), comparison_type,
		                                                  result_sel, count, true_sel, false_sel);
		return true;
	default:
		return false;
	}
}

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	ReferenceConstantComparison comparison;
	idx_t result_count;
	if (chunk && TryGetReferenceConstantComparison(expr, comparison) &&
	    TrySelectReferenceConstantComparison(*chunk, comparison, sel, count, true_sel, false_sel, result_count)) {
		return result_count;
	}

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
