#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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
	auto entries = vec.Validity(count);
	if (!entries.CanHaveNull()) {
		return;
	}
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	for (idx_t i = 0; i < count; ++i) {
		if (!entries.IsValid(i)) {
			null_mask.SetInvalid(sel->get_index(i));
		}
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, optional_ptr<const SelectionVector> sel, idx_t count,
                                   optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel,
                                   optional_ptr<ValidityMask> null_mask);

template <class OP>
static idx_t TemplatedSelectFlatColumnConstant(Vector &col, Vector &constant, bool col_left, const SelectionVector *sel,
                                               idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
#ifndef DUCKDB_SMALLER_BINARY
	const SelectionVector *effective_sel = sel ? sel : FlatVector::IncrementalSelectionVector();
	switch (col.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<int8_t, int8_t, OP, true>(col, constant, *sel, count,
				                                                                          true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<int8_t, int8_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                   true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<int8_t, int8_t, OP, false>(col, constant, *sel, count,
			                                                                           true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<int8_t, int8_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                   true_sel, false_sel);
	case PhysicalType::INT16:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<int16_t, int16_t, OP, true>(col, constant, *sel, count,
				                                                                            true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<int16_t, int16_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                     true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<int16_t, int16_t, OP, false>(col, constant, *sel, count,
			                                                                             true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<int16_t, int16_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                     true_sel, false_sel);
	case PhysicalType::INT32:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<int32_t, int32_t, OP, true>(col, constant, *sel, count,
				                                                                            true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<int32_t, int32_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                     true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<int32_t, int32_t, OP, false>(col, constant, *sel, count,
			                                                                             true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<int32_t, int32_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                     true_sel, false_sel);
	case PhysicalType::INT64:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<int64_t, int64_t, OP, true>(col, constant, *sel, count,
				                                                                            true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<int64_t, int64_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                     true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<int64_t, int64_t, OP, false>(col, constant, *sel, count,
			                                                                             true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<int64_t, int64_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                     true_sel, false_sel);
	case PhysicalType::UINT8:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<uint8_t, uint8_t, OP, true>(col, constant, *sel, count,
				                                                                            true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<uint8_t, uint8_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                     true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<uint8_t, uint8_t, OP, false>(col, constant, *sel, count,
			                                                                             true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<uint8_t, uint8_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                     true_sel, false_sel);
	case PhysicalType::UINT16:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<uint16_t, uint16_t, OP, true>(
				    col, constant, *sel, count, true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<uint16_t, uint16_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                       true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<uint16_t, uint16_t, OP, false>(col, constant, *sel, count,
			                                                                               true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<uint16_t, uint16_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                       true_sel, false_sel);
	case PhysicalType::UINT32:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<uint32_t, uint32_t, OP, true>(
				    col, constant, *sel, count, true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<uint32_t, uint32_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                       true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<uint32_t, uint32_t, OP, false>(col, constant, *sel, count,
			                                                                               true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<uint32_t, uint32_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                       true_sel, false_sel);
	case PhysicalType::UINT64:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<uint64_t, uint64_t, OP, true>(
				    col, constant, *sel, count, true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<uint64_t, uint64_t, OP, false, true>(col, constant, effective_sel, count,
			                                                                       true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<uint64_t, uint64_t, OP, false>(col, constant, *sel, count,
			                                                                               true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<uint64_t, uint64_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                       true_sel, false_sel);
	case PhysicalType::INT128:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<hugeint_t, hugeint_t, OP, true>(
				    col, constant, *sel, count, true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<hugeint_t, hugeint_t, OP, false, true>(col, constant, effective_sel,
			                                                                         count, true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<hugeint_t, hugeint_t, OP, false>(col, constant, *sel, count,
			                                                                                 true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<hugeint_t, hugeint_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                         true_sel, false_sel);
	case PhysicalType::UINT128:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<uhugeint_t, uhugeint_t, OP, true>(
				    col, constant, *sel, count, true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<uhugeint_t, uhugeint_t, OP, false, true>(col, constant, effective_sel,
			                                                                           count, true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<uhugeint_t, uhugeint_t, OP, false>(
			    col, constant, *sel, count, true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<uhugeint_t, uhugeint_t, OP, true, false>(constant, col, effective_sel, count,
		                                                                           true_sel, false_sel);
	case PhysicalType::FLOAT:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<float, float, OP, true>(col, constant, *sel, count,
				                                                                        true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<float, float, OP, false, true>(col, constant, effective_sel, count,
			                                                                 true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<float, float, OP, false>(col, constant, *sel, count,
			                                                                         true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<float, float, OP, true, false>(constant, col, effective_sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::DOUBLE:
		if (col_left) {
			if (sel) {
				return BinaryExecutor::SelectFlatColumnConstant<double, double, OP, true>(col, constant, *sel, count,
				                                                                          true_sel, false_sel);
			}
			return BinaryExecutor::SelectFlat<double, double, OP, false, true>(col, constant, effective_sel, count,
			                                                                   true_sel, false_sel);
		}
		if (sel) {
			return BinaryExecutor::SelectFlatColumnConstant<double, double, OP, false>(col, constant, *sel, count,
			                                                                           true_sel, false_sel);
		}
		return BinaryExecutor::SelectFlat<double, double, OP, true, false>(constant, col, effective_sel, count,
		                                                                   true_sel, false_sel);
	default:
		break;
	}
#endif
	return DConstants::INVALID_INDEX;
}

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
	case PhysicalType::ARRAY: {
		auto result_count = NestedSelectOperation<OP>(left, right, sel, count, true_sel, false_sel, null_mask);
		if (true_sel && result_count > 0) {
			std::sort(true_sel->data(), true_sel->data() + result_count);
		}
		if (false_sel) {
			idx_t false_count = count - result_count;
			if (false_count > 0) {
				std::sort(false_sel->data(), false_sel->data() + false_count);
			}
		}
		return result_count;
	}
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
	auto ldata = left.Validity(count);
	auto rdata = right.Validity(count);

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	idx_t remaining = 0;
	if (!ldata.CanHaveNull() && !rdata.CanHaveNull()) {
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
		if (!ldata.IsValid(i) || !rdata.IsValid(i)) {
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
	Vector l_not_null(Vector::Ref(left));
	Vector r_not_null(Vector::Ref(right));

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
	if (chunk) {
		bool ref_is_left = expr.left->GetExpressionClass() == ExpressionClass::BOUND_REF &&
		                   expr.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
		bool ref_is_right = !ref_is_left && expr.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		                    expr.right->GetExpressionClass() == ExpressionClass::BOUND_REF;
		if (ref_is_left || ref_is_right) {
			auto &ref = (ref_is_left ? *expr.left : *expr.right).Cast<BoundReferenceExpression>();
			idx_t const_child_idx = ref_is_left ? 1 : 0;
			auto *prebuilt = state->child_states[const_child_idx]->prebuilt_constant.get();
			if (prebuilt && ref.index < chunk->ColumnCount()) {
				auto &col = chunk->data[ref.index];
				if (col.GetVectorType() == VectorType::FLAT_VECTOR) {
					bool col_left = ref_is_left;
					idx_t result;
					switch (expr.GetExpressionType()) {
					case ExpressionType::COMPARE_EQUAL:
						result = TemplatedSelectFlatColumnConstant<duckdb::Equals>(col, *prebuilt, col_left, sel, count,
						                                                           true_sel, false_sel);
						break;
					case ExpressionType::COMPARE_NOTEQUAL:
						result = TemplatedSelectFlatColumnConstant<duckdb::NotEquals>(col, *prebuilt, col_left, sel,
						                                                              count, true_sel, false_sel);
						break;
					case ExpressionType::COMPARE_LESSTHAN:
						result = TemplatedSelectFlatColumnConstant<duckdb::LessThan>(col, *prebuilt, col_left, sel,
						                                                             count, true_sel, false_sel);
						break;
					case ExpressionType::COMPARE_GREATERTHAN:
						result = TemplatedSelectFlatColumnConstant<duckdb::GreaterThan>(col, *prebuilt, col_left, sel,
						                                                                count, true_sel, false_sel);
						break;
					case ExpressionType::COMPARE_LESSTHANOREQUALTO:
						result = TemplatedSelectFlatColumnConstant<duckdb::LessThanEquals>(
						    col, *prebuilt, col_left, sel, count, true_sel, false_sel);
						break;
					case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
						result = TemplatedSelectFlatColumnConstant<duckdb::GreaterThanEquals>(
						    col, *prebuilt, col_left, sel, count, true_sel, false_sel);
						break;
					default:
						result = DConstants::INVALID_INDEX;
						break;
					}
					if (result != DConstants::INVALID_INDEX) {
						return result;
					}
				}
			}
		}
	}

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
