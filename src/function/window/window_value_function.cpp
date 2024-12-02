#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/function/window/window_aggregator.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window/window_value_function.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ExclusionFilter
//===--------------------------------------------------------------------===//

//! Handles window exclusion by piggybacking on the filtering logic.
//! (needed for first_value, last_value, nth_value)
class ExclusionFilter {
public:
	ExclusionFilter(const WindowExcludeMode exclude_mode_p, idx_t total_count, const ValidityMask &src)
	    : mode(exclude_mode_p), mask_src(src) {
		mask.Initialize(total_count);

		// copy the data from mask_src
		FetchFromSource(0, total_count);
	}

	//! Copy the entries from mask_src to mask, in the index range [begin, end)
	void FetchFromSource(idx_t begin, idx_t end);
	//! Apply the current exclusion to the validity mask
	//! (offset is the current row's index within the chunk)
	void ApplyExclusion(DataChunk &bounds, idx_t row_idx, idx_t offset);
	//! Reset the validity mask to match mask_src
	//! (offset is the current row's index within the chunk)
	void ResetMask(idx_t row_idx, idx_t offset);

	//! The current peer group's begin
	idx_t curr_peer_begin;
	//! The current peer group's end
	idx_t curr_peer_end;
	//! The window exclusion mode
	WindowExcludeMode mode;
	//! The validity mask representing the exclusion
	ValidityMask mask;
	//! The validity mask upon which mask is based
	const ValidityMask &mask_src;
};

void ExclusionFilter::FetchFromSource(idx_t begin, idx_t end) {
	idx_t begin_entry_idx;
	idx_t end_entry_idx;
	idx_t idx_in_entry;
	mask.GetEntryIndex(begin, begin_entry_idx, idx_in_entry);
	mask.GetEntryIndex(end - 1, end_entry_idx, idx_in_entry);
	auto dst = mask.GetData() + begin_entry_idx;
	for (idx_t entry_idx = begin_entry_idx; entry_idx <= end_entry_idx; ++entry_idx) {
		*dst++ = mask_src.GetValidityEntry(entry_idx);
	}
}

void ExclusionFilter::ApplyExclusion(DataChunk &bounds, idx_t row_idx, idx_t offset) {
	// flip the bits in mask according to the window exclusion mode
	switch (mode) {
	case WindowExcludeMode::CURRENT_ROW:
		mask.SetInvalid(row_idx);
		break;
	case WindowExcludeMode::TIES:
	case WindowExcludeMode::GROUP: {
		if (curr_peer_end == row_idx || offset == 0) {
			// new peer group or input chunk: set entire peer group to invalid
			auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
			auto peer_end = FlatVector::GetData<const idx_t>(bounds.data[PEER_END]);
			curr_peer_begin = peer_begin[offset];
			curr_peer_end = peer_end[offset];
			for (idx_t i = curr_peer_begin; i < curr_peer_end; i++) {
				mask.SetInvalid(i);
			}
		}
		if (mode == WindowExcludeMode::TIES) {
			mask.Set(row_idx, mask_src.RowIsValid(row_idx));
		}
		break;
	}
	default:
		break;
	}
}

void ExclusionFilter::ResetMask(idx_t row_idx, idx_t offset) {
	// flip the bits that were modified in ApplyExclusion back
	switch (mode) {
	case WindowExcludeMode::CURRENT_ROW:
		mask.Set(row_idx, mask_src.RowIsValid(row_idx));
		break;
	case WindowExcludeMode::TIES:
		mask.SetInvalid(row_idx);
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case WindowExcludeMode::GROUP:
		if (curr_peer_end == row_idx + 1) {
			// if we've reached the peer group's end, restore the entire peer group
			FetchFromSource(curr_peer_begin, curr_peer_end);
		}
		break;
	default:
		break;
	}
}

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

	const column_t child_idx;
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
	}

	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	//! The corresponding global value state
	const WindowValueGlobalState &gvstate;
	//! The frame boundaries, used for EXCLUDE
	SubFrames frames;
	//! The exclusion filter handler
	unique_ptr<ExclusionFilter> exclusion_filter;
	//! The validity mask that combines both the NULLs and exclusion information
	optional_ptr<ValidityMask> ignore_nulls_exclude;

	//! The state used for reading the collection
	unique_ptr<WindowCursor> cursor;
};

void WindowValueLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	WindowExecutorBoundsState::Finalize(gstate, collection);

	// Set up the IGNORE NULLS state
	auto ignore_nulls = gvstate.ignore_nulls;
	if (gvstate.executor.wexpr.exclude_clause == WindowExcludeMode::NO_OTHER) {
		exclusion_filter = nullptr;
		ignore_nulls_exclude = ignore_nulls;
	} else {
		// create the exclusion filter based on ignore_nulls
		exclusion_filter =
		    make_uniq<ExclusionFilter>(gvstate.executor.wexpr.exclude_clause, gvstate.payload_count, *ignore_nulls);
		ignore_nulls_exclude = &exclusion_filter->mask;
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

WindowNtileExecutor::WindowNtileExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                         WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
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

void WindowNtileExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                           DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto partition_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[PARTITION_END]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		if (cursor.CellIsNull(0, row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = cursor.GetCell<int64_t>(0, row_idx);
			if (n_param < 1) {
				throw InvalidInputException("Argument for ntile must be greater than zero");
			}
			// With thanks from SQLite's ntileValueFunc()
			auto n_total = NumericCast<int64_t>(partition_end[i] - partition_begin[i]);
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= partition_begin[i]);
			auto adjusted_row_idx = NumericCast<int64_t>(row_idx - partition_begin[i]);
			// now compute the ntile
			int64_t n_large = n_total - n_param * n_size;
			int64_t i_small = n_large * (n_size + 1);
			int64_t result_ntile;

			D_ASSERT((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

			if (adjusted_row_idx < i_small) {
				result_ntile = 1 + adjusted_row_idx / (n_size + 1);
			} else {
				result_ntile = 1 + n_large + (adjusted_row_idx - i_small) / n_size;
			}
			// result has to be between [1, NTILE]
			D_ASSERT(result_ntile >= 1 && result_ntile <= n_param);
			rdata[i] = result_ntile;
		}
	}
}

//===--------------------------------------------------------------------===//
// WindowLeadLagLocalState
//===--------------------------------------------------------------------===//
class WindowLeadLagLocalState : public WindowValueLocalState {
public:
	explicit WindowLeadLagLocalState(const WindowValueGlobalState &gstate) : WindowValueLocalState(gstate) {
	}
};

WindowLeadLagExecutor::WindowLeadLagExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                             WindowSharedExpressions &shared)
    : WindowValueExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowLeadLagExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	const auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	return make_uniq<WindowLeadLagLocalState>(gvstate);
}

void WindowLeadLagExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                             DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gvstate = gstate.Cast<WindowValueGlobalState>();
	auto &ignore_nulls = gvstate.ignore_nulls;
	auto &llstate = lstate.Cast<WindowLeadLagLocalState>();
	auto &cursor = *llstate.cursor;

	WindowInputExpression leadlag_offset(eval_chunk, offset_idx);
	WindowInputExpression leadlag_default(eval_chunk, default_idx);

	bool can_shift = ignore_nulls->AllValid();
	if (wexpr.offset_expr) {
		can_shift = can_shift && wexpr.offset_expr->IsFoldable();
	}
	if (wexpr.default_expr) {
		can_shift = can_shift && wexpr.default_expr->IsFoldable();
	}

	auto partition_begin = FlatVector::GetData<const idx_t>(llstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(llstate.bounds.data[PARTITION_END]);
	const auto row_end = row_idx + count;
	for (idx_t i = 0; i < count;) {
		int64_t offset = 1;
		if (wexpr.offset_expr) {
			offset = leadlag_offset.GetCell<int64_t>(i);
		}
		int64_t val_idx = (int64_t)row_idx;
		if (wexpr.type == ExpressionType::WINDOW_LEAD) {
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
	auto exclude_mode = gvstate.executor.wexpr.exclude_clause;
	WindowAggregator::EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		for (const auto &frame : frames) {
			if (frame.start >= frame.end) {
				continue;
			}

			//	Same as NTH_VALUE(..., 1)
			idx_t n = 1;
			const auto first_idx =
			    WindowBoundariesState::FindNextStart(*lvstate.ignore_nulls_exclude, frame.start, frame.end, n);
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
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ApplyExclusion(lvstate.bounds, row_idx, i);
		}

		if (window_begin[i] >= window_end[i]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		idx_t n = 1;
		const auto last_idx =
		    WindowBoundariesState::FindPrevStart(*lvstate.ignore_nulls_exclude, window_begin[i], window_end[i], n);
		if (!n) {
			cursor.CopyCell(0, last_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
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

		for (const auto &frame : frames) {
			if (frame.start >= frame.end) {
				continue;
			}

			const auto nth_index =
			    WindowBoundariesState::FindNextStart(*lvstate.ignore_nulls_exclude, frame.start, frame.end, n);
			if (!n) {
				cursor.CopyCell(0, nth_index, result, i);
				return;
			}
		}
		FlatVector::SetNull(result, i, true);
	});
}

} // namespace duckdb
