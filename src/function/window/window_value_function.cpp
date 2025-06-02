#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/interpolate.hpp"
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
			const auto &sel = *child_data.sel;
			for (sel_t i = 0; i < coll_count; ++i) {
				const auto idx = sel.get_index(i);
				if (validity.RowIsValidUnsafe(idx)) {
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
			use_framing = true;

			//	If the argument order is prefix of the partition ordering,
			//	then we can just use the partition ordering.
			auto &wexpr = executor.wexpr;
			auto &arg_orders = executor.wexpr.arg_orders;
			const auto optimize = ClientConfig::GetConfig(executor.context).enable_optimizer;
			if (!optimize || BoundWindowExpression::GetSharedOrders(wexpr.orders, arg_orders) != arg_orders.size()) {
				//	"The ROW_NUMBER function can be computed by disambiguating duplicate elements based on their
				//	position in the input data, such that two elements never compare as equal."
				// 	Note: If the user specifies an partial secondary sort, the disambiguation will use the
				//	partition's row numbers, not the secondary sort's row numbers.
				row_tree = make_uniq<WindowTokenTree>(executor.context, arg_orders, executor.arg_order_idx,
				                                      payload_count, true);
			} else {
				// The value_tree is cheap to construct, so we just get rid of it if we now discover we don't need it.
				value_tree.reset();
			}
		}
	}

	//! Flag that we are using framing, even if we don't need the trees
	bool use_framing = false;

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

	auto frame_begin = FlatVector::GetData<const idx_t>(llstate.bounds.data[FRAME_BEGIN]);
	auto frame_end = FlatVector::GetData<const idx_t>(llstate.bounds.data[FRAME_END]);

	if (glstate.row_tree) {
		// TODO: Handle subframes (SelectNth can handle it but Rank can't)
		auto &frames = llstate.frames;
		frames.resize(1);
		auto &frame = frames[0];
		for (idx_t i = 0; i < count; ++i, ++row_idx) {
			int64_t offset = 1;
			if (wexpr.offset_expr) {
				if (leadlag_offset.CellIsNull(i)) {
					FlatVector::SetNull(result, i, true);
					continue;
				}
				offset = leadlag_offset.GetCell<int64_t>(i);
			}

			// (1) compute the ROW_NUMBER of the own row
			frame = FrameBounds(frame_begin[i], frame_end[i]);
			const auto own_row = glstate.row_tree->Rank(frame.start, frame.end, row_idx) - 1;
			// (2) adjust the row number by adding or subtracting an offset
			auto val_idx = NumericCast<int64_t>(own_row);
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
				if (nth_index.second) {
					//	Overflow
					FlatVector::SetNull(result, i, true);
				} else {
					cursor.CopyCell(0, nth_index.first, result, i);
				}
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

	// Only shift within the frame if we are using a shared ordering clause.
	if (glstate.use_framing) {
		partition_begin = frame_begin;
		partition_end = frame_end;
	}

	// We can't shift if we are ignoring NULLs (the rows may not be contiguous)
	// or if we are using framing (the frame may change on each row)
	auto &ignore_nulls = glstate.ignore_nulls;
	bool can_shift = ignore_nulls->AllValid() && !glstate.use_framing;
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
			if (leadlag_offset.CellIsNull(i)) {
				FlatVector::SetNull(result, i, true);
				++i;
				++row_idx;
				continue;
			}
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
				D_ASSERT(first_idx.second == 0);
				cursor.CopyCell(0, first_idx.first, result, i);
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
				auto n = frame_width - 1;
				auto last_idx = gvstate.value_tree->SelectNth(frames, n);
				if (last_idx.second && last_idx.second <= n) {
					//	Frame larger than data. Since we want last, we back off by the overflow
					n -= last_idx.second;
					last_idx = gvstate.value_tree->SelectNth(frames, n);
				}
				if (last_idx.second) {
					//	No last value - give up.
					FlatVector::SetNull(result, i, true);
				} else {
					cursor.CopyCell(0, last_idx.first, result, i);
				}
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
				if (nth_index.second) {
					// Past end of frame
					FlatVector::SetNull(result, i, true);
				} else {
					cursor.CopyCell(0, nth_index.first, result, i);
				}
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

//===--------------------------------------------------------------------===//
// WindowFillExecutor
//===--------------------------------------------------------------------===//
template <class TO, class FROM>
TO LossyFillCast(FROM val) {
	return LossyNumericCast<TO, FROM>(val);
}

template <>
double LossyFillCast(hugeint_t val) {
	double d;
	(void)Hugeint::TryCast(val, d);
	return d;
}

template <>
double LossyFillCast(uhugeint_t val) {
	double d;
	(void)Hugeint::TryCast(val, d);
	return d;
}

template <typename T>
static double FillSlopeFunc(WindowCursor &cursor, idx_t row_idx, idx_t prev_valid, idx_t next_valid) {
	//	Cast everything to doubles immediately so we can interpolate backwards (x < x0)
	const auto x = LossyFillCast<double>(cursor.GetCell<T>(0, row_idx));
	const auto x0 = LossyFillCast<double>(cursor.GetCell<T>(0, prev_valid));
	const auto x1 = LossyFillCast<double>(cursor.GetCell<T>(0, next_valid));

	return (x - x0) / (x1 - x0);
}

typedef double (*fill_slope_t)(WindowCursor &cursor, idx_t row_idx, idx_t prev_valid, idx_t next_valid);

static fill_slope_t GetFillSlopeFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
		return FillSlopeFunc<uint8_t>;
	case PhysicalType::UINT16:
		return FillSlopeFunc<uint16_t>;
	case PhysicalType::UINT32:
		return FillSlopeFunc<uint32_t>;
	case PhysicalType::UINT64:
		return FillSlopeFunc<uint64_t>;
	case PhysicalType::UINT128:
		return FillSlopeFunc<uhugeint_t>;
	case PhysicalType::INT8:
		return FillSlopeFunc<int8_t>;
	case PhysicalType::INT16:
		return FillSlopeFunc<int16_t>;
	case PhysicalType::INT32:
		return FillSlopeFunc<int32_t>;
	case PhysicalType::INT64:
		return FillSlopeFunc<int64_t>;
	case PhysicalType::INT128:
		return FillSlopeFunc<hugeint_t>;
	case PhysicalType::FLOAT:
		return FillSlopeFunc<float>;
	case PhysicalType::DOUBLE:
		return FillSlopeFunc<double>;
	default:
		throw InternalException("Unsupported FILL slow ordering type.");
	}
}

typedef void (*fill_interpolate_t)(Vector &result, idx_t i, WindowCursor &cursor, idx_t lo, idx_t hi, double slope);

template <typename T>
static void FillInterpolateFunc(Vector &result, idx_t i, WindowCursor &cursor, idx_t lo, idx_t hi, double slope) {
	const auto y0 = cursor.GetCell<T>(0, lo);
	const auto y1 = cursor.GetCell<T>(0, hi);

	FlatVector::SetNull(result, i, false);
	auto data = FlatVector::GetData<T>(result);
	data[i] = InterpolateOperator::Operation<T>(y0, slope, y1);
}

static fill_interpolate_t GetFillInterpolateFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
		return FillInterpolateFunc<uint8_t>;
	case PhysicalType::UINT16:
		return FillInterpolateFunc<uint16_t>;
	case PhysicalType::UINT32:
		return FillInterpolateFunc<uint32_t>;
	case PhysicalType::UINT64:
		return FillInterpolateFunc<uint64_t>;
	case PhysicalType::UINT128:
		return FillInterpolateFunc<uhugeint_t>;
	case PhysicalType::INT8:
		return FillInterpolateFunc<int8_t>;
	case PhysicalType::INT16:
		return FillInterpolateFunc<int16_t>;
	case PhysicalType::INT32:
		return FillInterpolateFunc<int32_t>;
	case PhysicalType::INT64:
		return FillInterpolateFunc<int64_t>;
	case PhysicalType::INT128:
		return FillInterpolateFunc<hugeint_t>;
	case PhysicalType::FLOAT:
		return FillInterpolateFunc<float>;
	case PhysicalType::DOUBLE:
		return FillInterpolateFunc<double>;
	default:
		throw InternalException("Unsupported FILL slow ordering type.");
	}
}

WindowFillExecutor::WindowFillExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                       WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {

	//	We use the range ordering, even if it has not been defined
	//	We don't need the validity mask because we have also requested the valid range for the ordering.
	if (!range_expr) {
		range_idx = shared.RegisterCollection(wexpr.orders[0].expression, false);
	}
}

static void WindowFillCopy(WindowCursor &cursor, Vector &result, idx_t count, idx_t row_idx, column_t col_idx = 0) {
	for (idx_t target_offset = 0; target_offset < count;) {
		const auto source_offset = cursor.Seek(row_idx);
		auto &source = cursor.chunk.data[col_idx];
		const auto copied = MinValue<idx_t>(cursor.chunk.size() - source_offset, count - target_offset);
		VectorOperations::Copy(source, result, source_offset + copied, source_offset, target_offset);
		target_offset += copied;
		row_idx += copied;
	}
}

void WindowFillExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                          DataChunk &, Vector &result, idx_t count, idx_t row_idx) const {

	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;

	//	Assume the best and just batch copy all the values
	WindowFillCopy(cursor, result, count, row_idx, 0);

	//	If all are valid, we are done
	UnifiedVectorFormat arg_data;
	result.ToUnifiedFormat(count, arg_data);
	if (arg_data.validity.AllValid()) {
		return;
	}

	//	Missing values - linear interpolation
	// auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[PARTITION_END]);
	auto valid_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[VALID_BEGIN]);
	auto valid_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[VALID_END]);

	auto fill_slope_func = GetFillSlopeFunction(wexpr.orders[0].expression->return_type);
	auto &range_cursor = *lvstate.range_cursor;

	auto fill_interpolate_func = GetFillInterpolateFunction(wexpr.children[0]->return_type);

	idx_t prev_partition = DConstants::INVALID_INDEX;
	idx_t prev_valid = DConstants::INVALID_INDEX;
	idx_t next_valid = DConstants::INVALID_INDEX;

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		//	Did we change partitions?
		if (prev_partition != partition_begin[i]) {
			prev_partition = partition_begin[i];
			prev_valid = DConstants::INVALID_INDEX;
			next_valid = DConstants::INVALID_INDEX;
		}

		//	If we are outside the validity range of the sort column, we can't use this value.
		if (row_idx < valid_begin[i] || valid_end[i] <= row_idx) {
			continue;
		}

		//	If this value is valid, track it for the next gap.
		const auto idx = arg_data.sel->get_index(i);
		if (arg_data.validity.RowIsValid(idx)) {
			prev_valid = row_idx;
			continue;
		}

		//	Missing value, so look for interpolation values

		//	Find the previous valid value, scanning backwards from the current row
		if (prev_valid == DConstants::INVALID_INDEX) {
			for (idx_t j = row_idx; j > valid_begin[i];) {
				if (!cursor.CellIsNull(0, --j)) {
					prev_valid = j;
					break;
				}
			}
		}

		//	If there is nothing beind us (missing early value) then scan forward
		if (prev_valid == DConstants::INVALID_INDEX) {
			for (idx_t j = row_idx + 1; j < valid_end[i]; ++j) {
				if (!cursor.CellIsNull(0, j)) {
					prev_valid = j;
					break;
				}
			}
		}

		//	No valid values!
		if (prev_valid == DConstants::INVALID_INDEX) {
			//	Skip to the next partition
			i += partition_end[i] - row_idx - 1;
			row_idx = partition_end[i] - 1;
			continue;
		}

		//	Find the next valid value after the previous
		next_valid = DConstants::INVALID_INDEX;
		for (idx_t j = prev_valid + 1; j < valid_end[i]; ++j) {
			if (!cursor.CellIsNull(0, j)) {
				next_valid = j;
				break;
			}
		}

		//	Nothing after, so scan backwards
		if (next_valid == DConstants::INVALID_INDEX) {
			for (idx_t j = prev_valid; j > valid_begin[i];) {
				if (!cursor.CellIsNull(0, --j)) {
					next_valid = j;
					//	Restore ordering
					std::swap(prev_valid, next_valid);
					break;
				}
			}
		}

		//	If we only have one value, then just copy it
		if (next_valid == DConstants::INVALID_INDEX) {
			cursor.CopyCell(0, prev_valid, result, i);
			continue;
		}

		//	Two values, so interpolate
		//	y = y0 + (x - x0) / (x1 - x0) * (y1 - y0)
		const auto slope = fill_slope_func(range_cursor, row_idx, prev_valid, next_valid);
		fill_interpolate_func(result, i, cursor, prev_valid, next_valid, slope);
	}
}

} // namespace duckdb
