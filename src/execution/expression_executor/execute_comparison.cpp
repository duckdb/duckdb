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
	Vector left(state->intermediate_chunk.data[0]);
	Vector right(state->intermediate_chunk.data[1]);

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
	// Default to the same as the final row
	template <typename OP>
	static idx_t Definite(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector *true_sel, SelectionVector &false_sel) {
		return Final<OP>(left, right, sel, count, true_sel, &false_sel);
	}

	// Select the possible rows that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static idx_t Possible(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector &true_sel, SelectionVector *false_sel) {
		return VectorOperations::NotDistinctFrom(left, right, sel, count, &true_sel, false_sel);
	}

	// Select the matching rows for the final position.
	// This needs to be specialised.
	template <typename OP>
	static idx_t Final(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                   SelectionVector *false_sel) {
		return 0;
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
	return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
}

// NotEquals must check everything that matched
template <>
idx_t PositionComparator::Possible<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                      idx_t count, SelectionVector &true_sel,
                                                      SelectionVector *false_sel) {
	return count;
}

template <>
idx_t PositionComparator::Final<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
idx_t PositionComparator::Definite<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector &false_sel) {
	return VectorOperations::DistinctLessThan(left, right, sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t PositionComparator::Definite<duckdb::GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                              idx_t count, SelectionVector *true_sel,
                                                              SelectionVector &false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, sel, count, true_sel, false_sel);
}

// Strict inequalities just use strict for both Definite and Final
template <>
idx_t PositionComparator::Final<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                     idx_t count, SelectionVector *true_sel,
                                                     SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, sel, count, true_sel, false_sel);
}

static inline idx_t SelectNotNull(VectorData &lvdata, VectorData &rvdata, const idx_t count,
                                  OptionalSelection &false_vec, SelectionVector &maybe_vec) {

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	if (!lvdata.validity.AllValid() || !rvdata.validity.AllValid()) {
		idx_t true_count = 0;
		idx_t false_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto lidx = lvdata.sel->get_index(i);
			const auto ridx = rvdata.sel->get_index(i);
			if (!lvdata.validity.RowIsValid(lidx) || !rvdata.validity.RowIsValid(ridx)) {
				false_vec.Append(false_count, i);
			} else {
				maybe_vec.set_index(true_count++, i);
			}
		}
		false_vec.Advance(false_count);

		return true_count;
	} else {
		for (idx_t i = 0; i < count; ++i) {
			maybe_vec.set_index(i, i);
		}
		return count;
	}
}

static void ScatterSelection(SelectionVector *target, const idx_t count, const SelectionVector *sel,
                             const SelectionVector &dense_vec) {
	if (!sel) {
		sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
	}

	if (target) {
		for (idx_t i = 0; i < count; ++i) {
			target->set_index(i, sel->get_index(dense_vec.get_index(i)));
		}
	}
}

template <typename OP>
static idx_t StructSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	// The select operations all use a dense pair of input vectors to partition
	// a selection vector in a single pass. But to implement progressive comparisons,
	// we have to make multiple passes, so we need to keep track of the original input positions
	// and then scatter the output selections when we are done.
	idx_t match_count = 0;
	idx_t no_match_count = count;

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULL LISTs
	VectorData lvdata, rvdata;
	left.Orrify(count, lvdata);
	right.Orrify(count, rvdata);

	// Make real selections for progressive comparisons
	SelectionVector true_vec(count);
	OptionalSelection true_opt(&true_vec);

	SelectionVector false_vec(count);
	OptionalSelection false_opt(&false_vec);

	SelectionVector maybe_vec(count);
	count = SelectNotNull(lvdata, rvdata, count, false_opt, maybe_vec);

	// If everything was NULL, fill in false_sel with sel
	if (count == 0) {
		ScatterSelection(false_sel, no_match_count, sel, FlatVector::INCREMENTAL_SELECTION_VECTOR);
		return count;
	}
	no_match_count -= count;

	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	for (idx_t col_no = 0; col_no < lchildren.size(); ++col_no) {
		SelectionVector left_sel(lvdata.sel->Slice(maybe_vec, count));
		SelectionVector right_sel(rvdata.sel->Slice(maybe_vec, count));

		// We use the maybe_vec as a dictionary to densify the children
		auto &lchild = *lchildren[col_no];
		Vector ldense(lchild, left_sel, count);

		auto &rchild = *rchildren[col_no];
		Vector rdense(rchild, right_sel, count);

		// Find everything that definitely matches
		auto true_count = PositionComparator::Definite<OP>(ldense, rdense, &maybe_vec, count, true_opt, maybe_vec);
		if (true_count > 0) {
			true_opt.Advance(true_count);
			match_count += true_count;
			count -= true_count;

			SelectionVector new_left_sel = SelectionVector(lvdata.sel->Slice(maybe_vec, count));
			SelectionVector new_right_sel = SelectionVector(lvdata.sel->Slice(maybe_vec, count));

			ldense.Slice(lchild, new_left_sel, count);
			rdense.Slice(rchild, new_right_sel, count);
		}

		if (col_no != lchildren.size() - 1) {
			// Find what might match on the next position
			true_count = PositionComparator::Possible<OP>(ldense, rdense, &maybe_vec, count, maybe_vec, false_opt);
			auto false_count = count - true_count;
			false_opt.Advance(false_count);
			no_match_count += false_count;

			count = true_count;
		} else {
			true_count = PositionComparator::Final<OP>(ldense, rdense, &maybe_vec, count, true_opt, false_opt);
			auto false_count = count - true_count;
			match_count += true_count;
			no_match_count += false_count;
		}
	}

	ScatterSelection(true_sel, match_count, sel, true_vec);
	ScatterSelection(false_sel, no_match_count, sel, false_vec);

	return match_count;
}

static void CompactListCursor(SelectionVector &cursor, VectorData &vdata, const idx_t pos,
                              const SelectionVector &undecided, const idx_t count) {
	const auto data = (const list_entry_t *)vdata.data;
	for (idx_t i = 0; i < count; ++i) {
		const auto idx = undecided.get_index(i);

		const auto lidx = vdata.sel->get_index(idx);
		const auto &entry = data[lidx];
		cursor.set_index(i, entry.offset + pos);
	}
}

template <typename OP>
static idx_t ListSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	// The select operations all use a dense pair of input vectors to partition
	// a selection vector in a single pass. But to implement progressive comparisons,
	// we have to make multiple passes, so we need to keep track of the original input positions
	// and then scatter the output selections when we are done.
	idx_t match_count = 0;
	idx_t no_match_count = count;

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULL LISTs
	VectorData lvdata, rvdata;
	left.Orrify(count, lvdata);
	right.Orrify(count, rvdata);

	// Make real selections for progressive comparisons
	SelectionVector true_vec(count);
	OptionalSelection true_opt(&true_vec);

	SelectionVector false_vec(count);
	OptionalSelection false_opt(&false_vec);

	SelectionVector maybe_vec(count);
	count = SelectNotNull(lvdata, rvdata, count, false_opt, maybe_vec);

	// If everything was NULL, fill in false_sel with sel
	if (count == 0) {
		ScatterSelection(false_sel, no_match_count, sel, FlatVector::INCREMENTAL_SELECTION_VECTOR);
		return count;
	}
	no_match_count -= count;

	// The cursors provide a means of mapping the selected list to a current position in that list.
	// We use them to create dictionary views of the children so we can vectorise the positional comparisons.
	SelectionVector lcursor(count);
	SelectionVector rcursor(count);

	const auto ldata = (const list_entry_t *)lvdata.data;
	const auto rdata = (const list_entry_t *)rvdata.data;

	Vector lchild(ListVector::GetEntry(left), lcursor, count);

	Vector rchild(ListVector::GetEntry(right), rcursor, count);

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
		// Set up the cursors for the current position
		CompactListCursor(lcursor, lvdata, pos, maybe_vec, count);
		CompactListCursor(rcursor, rvdata, pos, maybe_vec, count);

		// Tie-break the pairs where one of the LISTs is exhausted.
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t maybe_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = maybe_vec.get_index(i);
			const auto lidx = lvdata.sel->get_index(idx);
			const auto &lentry = ldata[lidx];
			const auto ridx = rvdata.sel->get_index(idx);
			const auto &rentry = rdata[ridx];
			if (lentry.length == pos || rentry.length == pos) {
				if (PositionComparator::TieBreak<OP>(lentry.length, rentry.length)) {
					true_opt.Append(true_count, idx);
				} else {
					false_opt.Append(false_count, idx);
				}
			} else {
				lcursor.set_index(maybe_count, lentry.offset + pos);
				rcursor.set_index(maybe_count, rentry.offset + pos);
				maybe_vec.set_index(maybe_count++, idx);
			}
		}
		true_opt.Advance(true_count);
		false_opt.Advance(false_count);
		count = maybe_count;
		match_count += true_count;
		no_match_count += false_count;

		// Find everything that definitely matches
		true_count = PositionComparator::Definite<OP>(lchild, rchild, &maybe_vec, count, true_opt, maybe_vec);
		true_opt.Advance(true_count);
		match_count += true_count;
		count -= true_count;

		// Compact the cursors if something changed.
		if (true_count > 0) {
			CompactListCursor(lcursor, lvdata, pos, maybe_vec, count);
			CompactListCursor(rcursor, rvdata, pos, maybe_vec, count);
		}

		// Find what might match on the next position
		maybe_count = PositionComparator::Possible<OP>(lchild, rchild, &maybe_vec, count, maybe_vec, false_opt);

		false_count = count - maybe_count;
		false_opt.Advance(false_count);
		no_match_count += false_count;

		count = maybe_count;
	}

	// Scatter the original selection to the output selections
	ScatterSelection(true_sel, match_count, sel, true_vec);
	ScatterSelection(false_sel, no_match_count, sel, false_vec);

	return match_count;
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
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::LessThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	// resolve the children
	Vector left(state->intermediate_chunk.data[0]);
	Vector right(state->intermediate_chunk.data[1]);

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
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
		throw NotImplementedException("Unknown comparison type!");
	}
}

} // namespace duckdb
