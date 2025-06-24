#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <algorithm>

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
}

static void UpdateNullMask(Vector &vec, optional_ptr<const SelectionVector> sel, idx_t count, ValidityMask &null_mask) {
	UnifiedVectorFormat vdata;
	vec.ToUnifiedFormat(count, vdata);

	if (vdata.validity.AllValid()) {
		return;
	}

	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}

	for (idx_t i = 0; i < count; ++i) {
		const auto ridx = sel->get_index(i);
		const auto vidx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(vidx)) {
			null_mask.SetInvalid(ridx);
		}
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                   optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                   optional_ptr<ValidityMask> null_mask);

template <class OP>
static idx_t TemplatedSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                      optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                      optional_ptr<ValidityMask> null_mask) {
	if (null_mask) {
		UpdateNullMask(left, sel, count, *null_mask);
		UpdateNullMask(right, sel, count, *null_mask);
	}
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return BinaryExecutor::Select<int8_t, int8_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                  false_sel.get());
	case PhysicalType::INT16:
		return BinaryExecutor::Select<int16_t, int16_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                    false_sel.get());
	case PhysicalType::INT32:
		return BinaryExecutor::Select<int32_t, int32_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                    false_sel.get());
	case PhysicalType::INT64:
		return BinaryExecutor::Select<int64_t, int64_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                    false_sel.get());
	case PhysicalType::UINT8:
		return BinaryExecutor::Select<uint8_t, uint8_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                    false_sel.get());
	case PhysicalType::UINT16:
		return BinaryExecutor::Select<uint16_t, uint16_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
	case PhysicalType::UINT32:
		return BinaryExecutor::Select<uint32_t, uint32_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
	case PhysicalType::UINT64:
		return BinaryExecutor::Select<uint64_t, uint64_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
	case PhysicalType::INT128:
		return BinaryExecutor::Select<hugeint_t, hugeint_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                        false_sel.get());
	case PhysicalType::UINT128:
		return BinaryExecutor::Select<uhugeint_t, uhugeint_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                          false_sel.get());
	case PhysicalType::FLOAT:
		return BinaryExecutor::Select<float, float, OP>(left, right, sel.get(), count, true_sel.get(), false_sel.get());
	case PhysicalType::DOUBLE:
		return BinaryExecutor::Select<double, double, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                  false_sel.get());
	case PhysicalType::INTERVAL:
		return BinaryExecutor::Select<interval_t, interval_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                          false_sel.get());
	case PhysicalType::VARCHAR:
		return BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel.get(), count, true_sel.get(),
		                                                      false_sel.get());
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
	case PhysicalType::ARRAY:
		return NestedSelectOperation<OP>(left, right, sel, count, true_sel, false_sel, null_mask);
	default:
		throw InternalException("Invalid type for comparison");
	}
}

struct NestedSelector {
	// Select the matching rows for the values of a nested type that are not both NULL.
	// Those semantics are the same as the corresponding non-distinct comparator
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
	                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
	                    optional_ptr<ValidityMask> null_mask) {
		throw InvalidTypeException(left.GetType(), "Invalid operation for nested SELECT");
	}
};

template <>
idx_t NestedSelector::Select<duckdb::Equals>(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                             idx_t count, optional_ptr<SelectionVector> true_sel,
                                             optional_ptr<SelectionVector> false_sel,
                                             optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t NestedSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                                idx_t count, optional_ptr<SelectionVector> true_sel,
                                                optional_ptr<SelectionVector> false_sel,
                                                optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t NestedSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                               idx_t count, optional_ptr<SelectionVector> true_sel,
                                               optional_ptr<SelectionVector> false_sel,
                                               optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctLessThan(left, right, sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t NestedSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right,
                                                     optional_ptr<const SelectionVector> sel, idx_t count,
                                                     optional_ptr<SelectionVector> true_sel,
                                                     optional_ptr<SelectionVector> false_sel,
                                                     optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctLessThanEquals(left, right, sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t NestedSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                                  idx_t count, optional_ptr<SelectionVector> true_sel,
                                                  optional_ptr<SelectionVector> false_sel,
                                                  optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThan(left, right, sel, count, true_sel, false_sel, null_mask);
}

template <>
idx_t NestedSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right,
                                                        optional_ptr<const SelectionVector> sel, idx_t count,
                                                        optional_ptr<SelectionVector> true_sel,
                                                        optional_ptr<SelectionVector> false_sel,
                                                        optional_ptr<ValidityMask> null_mask) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, sel, count, true_sel, false_sel, null_mask);
}

static inline idx_t SelectNotNull(Vector &left, Vector &right, const idx_t count, const SelectionVector &sel,
                                  SelectionVector &maybe_vec, OptionalSelection &false_opt,
                                  optional_ptr<ValidityMask> null_mask) {

	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(count, lvdata);
	right.ToUnifiedFormat(count, rvdata);

	auto &lmask = lvdata.validity;
	auto &rmask = rvdata.validity;

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
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
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto result_idx = sel.get_index(i);
		const auto lidx = lvdata.sel->get_index(i);
		const auto ridx = rvdata.sel->get_index(i);
		if (!lmask.RowIsValid(lidx) || !rmask.RowIsValid(ridx)) {
			if (null_mask) {
				null_mask->SetInvalid(result_idx);
			}
			false_opt.Append(false_count, result_idx);
		} else {
			//	Neither is NULL, distinguish values.
			slicer.set_index(remaining, i);
			maybe_vec.set_index(remaining++, result_idx);
		}
	}
	false_opt.Advance(false_count);

	if (remaining && remaining < count) {
		left.Slice(slicer, remaining);
		right.Slice(slicer, remaining);
	}

	return remaining;
}

static void ScatterSelection(optional_ptr<SelectionVector> target, const idx_t count,
                             const SelectionVector &dense_vec) {
	if (target) {
		for (idx_t i = 0; i < count; ++i) {
			target->set_index(i, dense_vec.get_index(i));
		}
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                   optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                   optional_ptr<ValidityMask> null_mask) {
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

	auto match_count = SelectNotNull(l_not_null, r_not_null, count, *sel, maybe_vec, false_opt, null_mask);
	auto no_match_count = count - match_count;
	count = match_count;

	//	Now that we have handled the NULLs, we can use the recursive nested comparator for the rest.
	match_count =
	    NestedSelector::Select<OP>(l_not_null, r_not_null, &maybe_vec, count, optional_ptr<SelectionVector>(true_opt),
	                               optional_ptr<SelectionVector>(false_opt), null_mask);
	no_match_count += (count - match_count);

	// Copy the buffered selections to the output selections
	ScatterSelection(true_sel, match_count, true_vec);
	ScatterSelection(false_sel, no_match_count, false_vec);

	return match_count;
}

idx_t VectorOperations::Equals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                               optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                               optional_ptr<ValidityMask> null_mask) {
	return TemplatedSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel, null_mask);
}

idx_t VectorOperations::NotEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                  optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                  optional_ptr<ValidityMask> null_mask) {
	return TemplatedSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel, null_mask);
}

idx_t VectorOperations::GreaterThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                    optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                    optional_ptr<ValidityMask> null_mask) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel, null_mask);
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                          idx_t count, optional_ptr<SelectionVector> true_sel,
                                          optional_ptr<SelectionVector> false_sel,
                                          optional_ptr<ValidityMask> null_mask) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel, null_mask);
}

idx_t VectorOperations::LessThan(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                 optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                 optional_ptr<ValidityMask> null_mask) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(right, left, sel, count, true_sel, false_sel, null_mask);
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel,
                                       idx_t count, optional_ptr<SelectionVector> true_sel,
                                       optional_ptr<SelectionVector> false_sel, optional_ptr<ValidityMask> null_mask) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(right, left, sel, count, true_sel, false_sel, null_mask);
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
