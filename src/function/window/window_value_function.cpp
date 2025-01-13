#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/function/window/window_aggregator.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/function/window/window_index_tree.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window/window_token_tree.hpp"
#include "duckdb/function/window/window_value_function.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowValueGlobalState
//===--------------------------------------------------------------------===//

class WindowValueGlobalState : public WindowExecutorGlobalState {
public:
	using WindowCollectionPtr = unique_ptr<WindowCollection>;
	WindowValueGlobalState(const WindowValueExecutor &executor, const idx_t payload_count,
	                       const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(executor, payload_count, partition_mask, order_mask), ignore_nulls(&all_valid),
	      child_idx(executor.child_idx) {

		if (!executor.arg_order_idx.empty()) {
			value_tree = make_uniq<WindowIndexTree>(executor.context, executor.wexpr.arg_orders, executor.arg_order_idx,
			                                        payload_count);
		}
	}

	void Finalize(CollectionPtr collection) {
		lock_guard<mutex> ignore_nulls_guard(lock);
		if (child_idx != DConstants::INVALID_INDEX && executor.wexpr.ignore_nulls) {
			ignore_nulls = &collection->validities[child_idx];
		}
	}

	// IGNORE NULLS
	mutex lock;
	ValidityMask all_valid;
	optional_ptr<ValidityMask> ignore_nulls;

	//! Copy of the executor child_idx
	const column_t child_idx;

	//! Merge sort tree to map unfiltered row number to value
	unique_ptr<WindowIndexTree> value_tree;
};

//===--------------------------------------------------------------------===//
// WindowValueLocalState
//===--------------------------------------------------------------------===//

//! A class representing the state of the first_value, last_value and nth_value functions
class WindowValueLocalState : public WindowExecutorBoundsState {
public:
	explicit WindowValueLocalState(const WindowValueGlobalState &gvstate)
	    : WindowExecutorBoundsState(gvstate), gvstate(gvstate) {
		WindowAggregatorLocalState::InitSubFrames(frames, gvstate.executor.wexpr.exclude_clause);

		if (gvstate.value_tree) {
			local_value = gvstate.value_tree->GetLocalState();
			if (gvstate.executor.wexpr.ignore_nulls) {
				sort_nulls.Initialize();
			}
		}
	}

	//! Accumulate the secondary sort values
	void Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
	          idx_t input_idx) override;
	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	//! The corresponding global value state
	const WindowValueGlobalState &gvstate;
	//! The optional sorting state for secondary sorts
	unique_ptr<WindowAggregatorState> local_value;
	//! Reusable selection vector for NULLs
	SelectionVector sort_nulls;
	//! The frame boundaries, used for EXCLUDE
	SubFrames frames;

	//! The state used for reading the collection
	unique_ptr<WindowCursor> cursor;
};

void WindowValueLocalState::Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                 idx_t input_idx) {
	WindowExecutorBoundsState::Sink(gstate, sink_chunk, coll_chunk, input_idx);

	if (local_value) {
		idx_t filtered = 0;
		optional_ptr<SelectionVector> filter_sel;

		// If we need to IGNORE NULLS for the child, and there are NULLs,
		// then build an SV to hold them
		const auto coll_count = coll_chunk.size();
		auto &child = coll_chunk.data[gvstate.child_idx];
		UnifiedVectorFormat child_data;
		child.ToUnifiedFormat(coll_count, child_data);
		const auto &validity = child_data.validity;
		if (gstate.executor.wexpr.ignore_nulls && !validity.AllValid()) {
			for (sel_t i = 0; i < coll_count; ++i) {
				if (validity.RowIsValidUnsafe(i)) {
					sort_nulls[filtered++] = i;
				}
			}
			filter_sel = &sort_nulls;
		}

		auto &value_state = local_value->Cast<WindowIndexTreeLocalState>();
		value_state.SinkChunk(sink_chunk, input_idx, filter_sel, filtered);
	}
}

void WindowValueLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	WindowExecutorBoundsState::Finalize(gstate, collection);

	if (local_value) {
		auto &value_state = local_value->Cast<WindowIndexTreeLocalState>();
		value_state.Sort();
		value_state.index_tree.Build();
	}

	// Prepare to scan
	if (!cursor && gvstate.child_idx != DConstants::INVALID_INDEX) {
		cursor = make_uniq<WindowCursor>(*collection, gvstate.child_idx);
	}
}

//===--------------------------------------------------------------------===//
// WindowValueExecutor
//===--------------------------------------------------------------------===//
WindowValueExecutor::WindowValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                         WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {

	for (const auto &order : wexpr.arg_orders) {
		arg_order_idx.emplace_back(shared.RegisterSink(order.expression));
	}

	//	The children have to be handled separately because only the first one is global
	if (!wexpr.children.empty()) {
		child_idx = shared.RegisterCollection(wexpr.children[0], wexpr.ignore_nulls);

		if (wexpr.children.size() > 1) {
			nth_idx = shared.RegisterEvaluate(wexpr.children[1]);
		}
	}

	offset_idx = shared.RegisterEvaluate(wexpr.offset_expr);
	default_idx = shared.RegisterEvaluate(wexpr.default_expr);
}

unique_ptr<WindowExecutorGlobalState> WindowValueExecutor::GetGlobalState(const idx_t payload_count,
                                                                          const ValidityMask &partition_mask,
                                                                          const ValidityMask &order_mask) const {
	return make_uniq<WindowValueGlobalState>(*this, payload_count, partition_mask, order_mask);
}

void WindowValueExecutor::Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                   CollectionPtr collection) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	gvstate.Finalize(collection);

	WindowExecutor::Finalize(gstate, lstate, collection);
}

unique_ptr<WindowExecutorLocalState> WindowValueExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	const auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	return make_uniq<WindowValueLocalState>(gvstate);
}

//===--------------------------------------------------------------------===//
// WindowLeadLagGlobalState
//===--------------------------------------------------------------------===//
// The functions LEAD and LAG can be extended to a windowed version with
// two independent ORDER BY clauses just like first_value and other value
// functions.
// To evaluate a windowed LEAD/LAG, one has to (1) compute the ROW_NUMBER
// of the own row, (2) adjust the row number by adding or subtracting an
// offset, (3) find the row at that offset, and (4) evaluate the expression
// provided to LEAD/LAG on this row. One can use the algorithm from Section
// 4.4 to determine the row number of the own row (step 1) and the
// algorithm from Section 4.5 to find the row with the adjusted position
// (step 3). Both algorithms are in O(𝑛 log𝑛), so the overall algorithm
// for LEAD/LAG is also O(𝑛 log𝑛).
//
// 4.4: unique WindowTokenTree
// 4.5: WindowIndexTree

class WindowLeadLagGlobalState : public WindowValueGlobalState {
public:
	explicit WindowLeadLagGlobalState(const WindowValueExecutor &executor, const idx_t payload_count,
	                                  const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowValueGlobalState(executor, payload_count, partition_mask, order_mask) {

		if (value_tree) {
			//	"The ROW_NUMBER function can be computed by disambiguating duplicate elements based on their position in
			//	the input data, such that two elements never compare as equal."
			// 	Note: If the user specifies an partial secondary sort, the disambiguation will use the
			//	partition's row numbers, not the secondary sort's row numbers.
			row_tree = make_uniq<WindowTokenTree>(executor.context, executor.wexpr.arg_orders, executor.arg_order_idx,
			                                      payload_count, true);
		}
	}

	//! Merge sort tree to map partition offset to row number (algorithm from Section 4.5)
	unique_ptr<WindowTokenTree> row_tree;
};

//===--------------------------------------------------------------------===//
// WindowLeadLagLocalState
//===--------------------------------------------------------------------===//
class WindowLeadLagLocalState : public WindowValueLocalState {
public:
	explicit WindowLeadLagLocalState(const WindowLeadLagGlobalState &gstate) : WindowValueLocalState(gstate) {
		if (gstate.row_tree) {
			local_row = gstate.row_tree->GetLocalState();
		}
	}

	//! Accumulate the secondary sort values
	void Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
	          idx_t input_idx) override;
	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	//! The optional sorting state for the secondary sort row mapping
	unique_ptr<WindowAggregatorState> local_row;
};

void WindowLeadLagLocalState::Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                   idx_t input_idx) {
	WindowValueLocalState::Sink(gstate, sink_chunk, coll_chunk, input_idx);

	if (local_row) {
		idx_t filtered = 0;
		optional_ptr<SelectionVector> filter_sel;

		auto &row_state = local_row->Cast<WindowMergeSortTreeLocalState>();
		row_state.SinkChunk(sink_chunk, input_idx, filter_sel, filtered);
	}
}

void WindowLeadLagLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	WindowValueLocalState::Finalize(gstate, collection);

	if (local_row) {
		auto &row_state = local_row->Cast<WindowMergeSortTreeLocalState>();
		row_state.Sort();
		row_state.window_tree.Build();
	}
}

//===--------------------------------------------------------------------===//
// WindowLeadLagExecutor
//===--------------------------------------------------------------------===//
WindowLeadLagExecutor::WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                             WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorGlobalState> WindowLeadLagExecutor::GetGlobalState(const idx_t payload_count,
                                                                            const ValidityMask &partition_mask,
                                                                            const ValidityMask &order_mask) const {
	return make_uniq<WindowLeadLagGlobalState>(*this, payload_count, partition_mask, order_mask);
}

unique_ptr<WindowExecutorLocalState>
WindowLeadLagExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	const auto &glstate = gstate.Cast<WindowLeadLagGlobalState>();
	return make_uniq<WindowLeadLagLocalState>(glstate);
}

void WindowLeadLagExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                             DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &glstate = gstate.Cast<WindowLeadLagGlobalState>();
	auto &llstate = lstate.Cast<WindowLeadLagLocalState>();
	auto &cursor = *llstate.cursor;

	WindowInputExpression leadlag_offset(eval_chunk, offset_idx);
	WindowInputExpression leadlag_default(eval_chunk, default_idx);

	if (glstate.row_tree) {
		auto frame_begin = FlatVector::GetData<const idx_t>(llstate.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(llstate.bounds.data[FRAME_END]);
		// TODO: Handle subframes.
		auto &frames = llstate.frames;
		frames.resize(1);
		auto &frame = frames[0];
		for (idx_t i = 0; i < count; ++i, ++row_idx) {
			// (1) compute the ROW_NUMBER of the own row
			frame = FrameBounds(frame_begin[i], frame_end[i]);
			const auto own_row = glstate.row_tree->Rank(frame.start, frame.end, row_idx) - 1;
			// (2) adjust the row number by adding or subtracting an offset
			auto val_idx = NumericCast<int64_t>(own_row);
			int64_t offset = 1;
			if (wexpr.offset_expr) {
				offset = leadlag_offset.GetCell<int64_t>(i);
			}
			if (wexpr.GetExpressionType() == ExpressionType::WINDOW_LEAD) {
				val_idx = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(val_idx, offset);
			} else {
				val_idx = SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(val_idx, offset);
			}
			const auto frame_width = NumericCast<int64_t>(frame.end - frame.start);
			if (val_idx >= 0 && val_idx < frame_width) {
				// (3) find the row at that offset
				const auto n = NumericCast<idx_t>(val_idx);
				const auto nth_index = glstate.value_tree->SelectNth(frames, n);
				// (4) evaluate the expression provided to LEAD/LAG on this row.
				cursor.CopyCell(0, nth_index, result, i);
			} else if (wexpr.default_expr) {
				leadlag_default.CopyCell(result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
		}
		return;
	}

	auto partition_begin = FlatVector::GetData<const idx_t>(llstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(llstate.bounds.data[PARTITION_END]);

	auto &ignore_nulls = glstate.ignore_nulls;
	bool can_shift = ignore_nulls->AllValid();
	if (wexpr.offset_expr) {
		can_shift = can_shift && wexpr.offset_expr->IsFoldable();
	}
	if (wexpr.default_expr) {
		can_shift = can_shift && wexpr.default_expr->IsFoldable();
	}

	const auto row_end = row_idx + count;
	for (idx_t i = 0; i < count;) {
		int64_t offset = 1;
		if (wexpr.offset_expr) {
			offset = leadlag_offset.GetCell<int64_t>(i);
		}
		int64_t val_idx = (int64_t)row_idx;
		if (wexpr.GetExpressionType() == ExpressionType::WINDOW_LEAD) {
			val_idx = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(val_idx, offset);
		} else {
			val_idx = SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(val_idx, offset);
		}

		idx_t delta = 0;
		if (val_idx < (int64_t)row_idx) {
			// Count backwards
			delta = idx_t(row_idx - idx_t(val_idx));
			val_idx = int64_t(WindowBoundariesState::FindPrevStart(*ignore_nulls, partition_begin[i], row_idx, delta));
		} else if (val_idx > (int64_t)row_idx) {
			delta = idx_t(idx_t(val_idx) - row_idx);
			val_idx =
			    int64_t(WindowBoundariesState::FindNextStart(*ignore_nulls, row_idx + 1, partition_end[i], delta));
		}
		// else offset is zero, so don't move.

		if (can_shift) {
			const auto target_limit = MinValue(partition_end[i], row_end) - row_idx;
			if (!delta) {
				//	Copy source[index:index+width] => result[i:]
				auto index = NumericCast<idx_t>(val_idx);
				const auto source_limit = partition_end[i] - index;
				auto width = MinValue(source_limit, target_limit);
				// We may have to scan multiple blocks here, so loop until we have copied everything
				const idx_t col_idx = 0;
				while (width) {
					const auto source_offset = cursor.Seek(index);
					auto &source = cursor.chunk.data[col_idx];
					const auto copied = MinValue<idx_t>(cursor.chunk.size() - source_offset, width);
					VectorOperations::Copy(source, result, source_offset + copied, source_offset, i);
					i += copied;
					row_idx += copied;
					index += copied;
					width -= copied;
				}
			} else if (wexpr.default_expr) {
				const auto width = MinValue(delta, target_limit);
				leadlag_default.CopyCell(result, i, width);
				i += width;
				row_idx += width;
			} else {
				for (idx_t nulls = MinValue(delta, target_limit); nulls--; ++i, ++row_idx) {
					FlatVector::SetNull(result, i, true);
				}
			}
		} else {
			if (!delta) {
				cursor.CopyCell(0, NumericCast<idx_t>(val_idx), result, i);
			} else if (wexpr.default_expr) {
				leadlag_default.CopyCell(result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			++i;
			++row_idx;
		}
	}
}

WindowFirstValueExecutor::WindowFirstValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                   WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

void WindowFirstValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                DataChunk &eval_chunk, Vector &result, idx_t count,
                                                idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto &bounds = lvstate.bounds;
	auto &frames = lvstate.frames;
	auto &ignore_nulls = *gvstate.ignore_nulls;
	auto exclude_mode = gvstate.executor.wexpr.exclude_clause;
	WindowAggregator::EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		if (gvstate.value_tree) {
			idx_t frame_width = 0;
			for (const auto &frame : frames) {
				frame_width += frame.end - frame.start;
			}

			if (frame_width) {
				const auto first_idx = gvstate.value_tree->SelectNth(frames, 0);
				cursor.CopyCell(0, first_idx, result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			return;
		}

		for (const auto &frame : frames) {
			if (frame.start >= frame.end) {
				continue;
			}

			//	Same as NTH_VALUE(..., 1)
			idx_t n = 1;
			const auto first_idx = WindowBoundariesState::FindNextStart(ignore_nulls, frame.start, frame.end, n);
			if (!n) {
				cursor.CopyCell(0, first_idx, result, i);
				return;
			}
		}

		// Didn't find one
		FlatVector::SetNull(result, i, true);
	});
}

WindowLastValueExecutor::WindowLastValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

void WindowLastValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto &bounds = lvstate.bounds;
	auto &frames = lvstate.frames;
	auto &ignore_nulls = *gvstate.ignore_nulls;
	auto exclude_mode = gvstate.executor.wexpr.exclude_clause;
	WindowAggregator::EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		if (gvstate.value_tree) {
			idx_t frame_width = 0;
			for (const auto &frame : frames) {
				frame_width += frame.end - frame.start;
			}

			if (frame_width) {
				const auto last_idx = gvstate.value_tree->SelectNth(frames, frame_width - 1);
				cursor.CopyCell(0, last_idx, result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			return;
		}

		for (idx_t f = frames.size(); f-- > 0;) {
			const auto &frame = frames[f];
			if (frame.start >= frame.end) {
				continue;
			}

			idx_t n = 1;
			const auto last_idx = WindowBoundariesState::FindPrevStart(ignore_nulls, frame.start, frame.end, n);
			if (!n) {
				cursor.CopyCell(0, last_idx, result, i);
				return;
			}
		}

		// Didn't find one
		FlatVector::SetNull(result, i, true);
	});
}

WindowNthValueExecutor::WindowNthValueExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                               WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

void WindowNthValueExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto &bounds = lvstate.bounds;
	auto &frames = lvstate.frames;
	auto &ignore_nulls = *gvstate.ignore_nulls;
	auto exclude_mode = gvstate.executor.wexpr.exclude_clause;
	D_ASSERT(cursor.chunk.ColumnCount() == 1);
	WindowInputExpression nth_col(eval_chunk, nth_idx);
	WindowAggregator::EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
		// returns NULL if there is no such row.
		if (nth_col.CellIsNull(i)) {
			FlatVector::SetNull(result, i, true);
			return;
		}
		auto n_param = nth_col.GetCell<int64_t>(i);
		if (n_param < 1) {
			FlatVector::SetNull(result, i, true);
			return;
		}

		//	Decrement as we go along.
		auto n = idx_t(n_param);

		if (gvstate.value_tree) {
			idx_t frame_width = 0;
			for (const auto &frame : frames) {
				frame_width += frame.end - frame.start;
			}

			if (n < frame_width) {
				const auto nth_index = gvstate.value_tree->SelectNth(frames, n - 1);
				cursor.CopyCell(0, nth_index, result, i);
			} else {
				FlatVector::SetNull(result, i, true);
			}
			return;
		}

		for (const auto &frame : frames) {
			if (frame.start >= frame.end) {
				continue;
			}

			const auto nth_index = WindowBoundariesState::FindNextStart(ignore_nulls, frame.start, frame.end, n);
			if (!n) {
				cursor.CopyCell(0, nth_index, result, i);
				return;
			}
		}
		FlatVector::SetNull(result, i, true);
	});
}

} // namespace duckdb
