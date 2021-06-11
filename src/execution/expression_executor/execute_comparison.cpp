#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundComparisonExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.left.get());
	result->AddChild(expr.right.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// resolve the children
	Vector left, right;
	left.Reference(state->intermediate_chunk.data[0]);
	right.Reference(state->intermediate_chunk.data[1]);

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
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
		throw NotImplementedException("Unknown comparison type!");
	}
}

template <typename OP>
static idx_t StructSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel);

template <typename OP>
static idx_t ListSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel);

template <class OP>
static idx_t TemplatedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                      SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return BinaryExecutor::Select<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return BinaryExecutor::Select<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return BinaryExecutor::Select<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return BinaryExecutor::Select<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return BinaryExecutor::Select<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return BinaryExecutor::Select<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return BinaryExecutor::Select<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return BinaryExecutor::Select<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return BinaryExecutor::Select<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::POINTER:
		return BinaryExecutor::Select<uintptr_t, uintptr_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return BinaryExecutor::Select<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return BinaryExecutor::Select<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return BinaryExecutor::Select<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::STRUCT:
		return StructSelectOperation<OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::LIST:
		return ListSelectOperation<OP>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InvalidTypeException(left.GetType(), "Invalid type for comparison");
	}
}

struct PositionComparator {

	// Select the rows that definitely match.
	template <typename OP>
	static idx_t Definite(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector *true_sel, SelectionVector &false_sel) {
		return TemplatedSelectOperation<OP>(left, right, sel, count, true_sel, &false_sel);
	}

	// Select the possible rows that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static idx_t Possible(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector &true_sel, SelectionVector *false_sel) {
		return VectorOperations::SelectNotDistinctFrom(left, right, sel, count, &true_sel, false_sel);
	}

	// Select the matching rows for the final position.
	// Usually the OP can tie break correctly
	template <typename OP>
	static idx_t Final(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                   SelectionVector *false_sel) {
		return TemplatedSelectOperation<OP>(left, right, sel, count, true_sel, false_sel);
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos);
	}
};

// Equals must always check every column
template <>
idx_t PositionComparator::Definite<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                   SelectionVector *true_sel, SelectionVector &false_sel) {
	return 0;
}

template <>
idx_t PositionComparator::Final<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::SelectNotDistinctFrom(left, right, sel, count, true_sel, false_sel);
}

// NotEquals must check everything that matched
template <>
idx_t PositionComparator::Definite<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                      idx_t count, SelectionVector *true_sel,
                                                      SelectionVector &false_sel) {
	return VectorOperations::SelectDistinctFrom(left, right, sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Possible<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                      idx_t count, SelectionVector &true_sel,
                                                      SelectionVector *false_sel) {
	return count;
}

template <>
idx_t PositionComparator::Final<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::SelectDistinctFrom(left, right, sel, count, true_sel, false_sel);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
idx_t PositionComparator::Definite<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector &false_sel) {
	return TemplatedSelectOperation<duckdb::LessThan>(left, right, sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Definite<duckdb::GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                              idx_t count, SelectionVector *true_sel,
                                                              SelectionVector &false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, &false_sel);
}

static inline SelectionVector *InitializeSelection(SelectionVector &vec, SelectionVector *sel) {
	if (sel) {
		vec.Initialize(sel->data());
		sel = &vec;
	}
	return sel;
}

static inline void AppendSelection(SelectionVector *sel, idx_t &count, const idx_t idx) {
	if (sel) {
		sel->set_index(count, idx);
	}
	++count;
}

static inline void AdvanceSelection(SelectionVector *sel, idx_t completed) {
	if (sel) {
		sel->Initialize(sel->data() + completed);
	}
}

static inline const SelectionVector *SelectNotNull(VectorData &ldata, VectorData &rdata, idx_t &count,
                                                   const SelectionVector *sel, SelectionVector *false_sel,
                                                   SelectionVector &maybe_vec) {

	//	We need multiple, real selections
	if (!sel) {
		sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	}

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	if (!ldata.validity.AllValid() || !rdata.validity.AllValid()) {
		idx_t true_count = 0;
		idx_t false_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel->get_index(i);
			if (!ldata.validity.RowIsValid(idx) || !rdata.validity.RowIsValid(idx)) {
				AppendSelection(false_sel, false_count, idx);
			} else {
				AppendSelection(&maybe_vec, true_count, idx);
			}
		}
		AdvanceSelection(false_sel, false_count);

		sel = &maybe_vec;
		count = true_count;
	}

	return sel;
}

template <typename OP>
static idx_t StructSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t result = 0;

	// Incrementally fill in successes and failures as we discover them
	SelectionVector true_vec;
	true_sel = InitializeSelection(true_vec, true_sel);

	SelectionVector false_vec;
	false_sel = InitializeSelection(false_vec, false_sel);

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	VectorData ldata, rdata;
	left.Orrify(count, ldata);
	right.Orrify(count, rdata);

	SelectionVector maybe_vec(count);
	sel = SelectNotNull(ldata, rdata, count, sel, false_sel, maybe_vec);

	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	idx_t col_no = 0;
	for (; col_no < lchildren.size() - 1; ++col_no) {
		auto &lchild = *lchildren[col_no];
		auto &rchild = *rchildren[col_no];

		// Find everything that definitely matches
		auto definite = PositionComparator::Definite<OP>(lchild, rchild, sel, count, true_sel, maybe_vec);
		AdvanceSelection(true_sel, definite);
		count -= definite;
		result += definite;

		// Find what might match on the next position
		idx_t possible = 0;
		if (definite > 0) {
			possible = PositionComparator::Possible<OP>(lchild, rchild, &maybe_vec, count, maybe_vec, false_sel);
			sel = &maybe_vec;
		} else {
			// If there were no definite matches, then for speed,
			// maybe_vec may not have been filled in.
			possible = PositionComparator::Possible<OP>(lchild, rchild, sel, count, maybe_vec, false_sel);

			// If everything is still possible, then for speed,
			// maybe_vec may not have been filled in.
			if (possible != count) {
				sel = &maybe_vec;
			}
		}

		AdvanceSelection(false_sel, (count - possible));

		count = possible;
	}

	//	Find everything that matches the last column exactly
	auto &lchild = *lchildren[col_no];
	auto &rchild = *rchildren[col_no];
	result += PositionComparator::Final<OP>(lchild, rchild, sel, count, true_sel, false_sel);

	return result;
}

template <typename OP>
static idx_t ListSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t result = 0;

	// Incrementally fill in successes and failures as we discover them
	SelectionVector true_vec;
	true_sel = InitializeSelection(true_vec, true_sel);

	SelectionVector false_vec;
	false_sel = InitializeSelection(false_vec, false_sel);

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULL LISTs
	VectorData lvdata, rvdata;
	left.Orrify(count, lvdata);
	right.Orrify(count, rvdata);

	SelectionVector maybe_vec(count);
	sel = SelectNotNull(lvdata, rvdata, count, sel, false_sel, maybe_vec);

	if (count == 0) {
		return count;
	}

	// The cursors provide a means of mapping the selected list to a current position in that list.
	// We use them to create dictionary views of the children so we can vectorise the positional comparisons.
	// Note that they only need to be as large as the parent because only one entry is active per LIST.
	SelectionVector lcursor(count);
	SelectionVector rcursor(count);

	const auto ldata = (const list_entry_t *)lvdata.data;
	const auto rdata = (const list_entry_t *)rvdata.data;

	for (idx_t i = 0; i < count; ++i) {
		const idx_t idx = sel->get_index(i);
		const auto &lentry = ldata[idx];
		const auto &rentry = rdata[idx];
		lcursor.set_index(idx, lentry.offset);
		rcursor.set_index(idx, rentry.offset);
	}

	Vector lchild;
	lchild.Slice(ListVector::GetEntry(left), lcursor, count);

	Vector rchild;
	rchild.Slice(ListVector::GetEntry(right), rcursor, count);

	// To perform the positional comparison, we use a vectorisation of the following algorithm:
	// bool CompareLists(T *left, idx_t nleft, T *right, nright) {
	// 	for (idx_t pos = 0; ; ++pos) {
	// 		if (nleft == pos || nright == pos)
	// 			return OP::TieBreak(nleft, nright);
	// 		if (OP::Definite(*left, *right))
	// 			return true;
	// 		if (!OP::Maybe(*left, *right))
	// 			return false;
	// 		}
	//	 	++left, ++right;
	// 	}
	// }
	for (idx_t pos = 0; count > 0; ++pos) {
		// Tie-break the pairs where one of the LISTs is exhausted.
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t remaining = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel->get_index(i);
			const auto &lentry = ldata[idx];
			const auto &rentry = rdata[idx];
			if (lentry.length == pos || rentry.length == pos) {
				if (PositionComparator::TieBreak<OP>(lentry.length, rentry.length)) {
					AppendSelection(true_sel, true_count, idx);
				} else {
					AppendSelection(false_sel, false_count, idx);
				}
			} else {
				AppendSelection(&maybe_vec, remaining, idx);
			}
		}
		AdvanceSelection(true_sel, true_count);
		AdvanceSelection(false_sel, false_count);
		count = remaining;
		sel = &maybe_vec;
		result += true_count;

		// Find everything that definitely matches
		auto definite = PositionComparator::Definite<OP>(lchild, rchild, sel, count, true_sel, maybe_vec);
		AdvanceSelection(true_sel, definite);
		result += definite;
		count -= definite;

		// Find what might match on the next position
		idx_t possible = 0;
		if (definite > 0) {
			possible = PositionComparator::Possible<OP>(lchild, rchild, &maybe_vec, count, maybe_vec, false_sel);
			sel = &maybe_vec;
		} else {
			// If there were no definite matches, then for speed,
			// maybe_vec may not have been filled in, so we reuse sel.
			possible = PositionComparator::Possible<OP>(lchild, rchild, sel, count, maybe_vec, false_sel);

			// If everything is still possible, then for speed,
			// maybe_vec may not have been filled in, so we reuse sel.
			if (possible != count) {
				sel = &maybe_vec;
			}
		}
		AdvanceSelection(false_sel, (count - possible));
		count = possible;

		// Increment the cursors
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel->get_index(i);
			lcursor.set_index(idx, lcursor.get_index(idx) + 1);
			rcursor.set_index(idx, rcursor.get_index(idx) + 1);
		}
	}

	return result;
}

idx_t VectorOperations::Equals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::NotEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	// resolve the children
	Vector left, right;
	left.Reference(state->intermediate_chunk.data[0]);
	right.Reference(state->intermediate_chunk.data[1]);

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::SelectDistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::SelectNotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}

} // namespace duckdb
