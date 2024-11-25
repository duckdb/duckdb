#include "duckdb/function/window/window_executor.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/function/window/window_boundaries_state.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "duckdb/common/array.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowExecutorBoundsState
//===--------------------------------------------------------------------===//
class WindowExecutorBoundsState : public WindowExecutorLocalState {
public:
	explicit WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate);
	~WindowExecutorBoundsState() override {
	}

	virtual void UpdateBounds(WindowExecutorGlobalState &gstate, idx_t row_idx, DataChunk &eval_chunk,
	                          optional_ptr<WindowCursor> range);

	// Frame management
	const ValidityMask &partition_mask;
	const ValidityMask &order_mask;
	DataChunk bounds;
	WindowBoundariesState state;
};

WindowExecutorBoundsState::WindowExecutorBoundsState(const WindowExecutorGlobalState &gstate)
    : WindowExecutorLocalState(gstate), partition_mask(gstate.partition_mask), order_mask(gstate.order_mask),
      state(gstate.executor.wexpr, gstate.payload_count) {
	vector<LogicalType> bounds_types(8, LogicalType(LogicalTypeId::UBIGINT));
	bounds.Initialize(Allocator::Get(gstate.executor.context), bounds_types);
}

void WindowExecutorBoundsState::UpdateBounds(WindowExecutorGlobalState &gstate, idx_t row_idx, DataChunk &eval_chunk,
                                             optional_ptr<WindowCursor> range) {
	// Evaluate the row-level arguments
	WindowInputExpression boundary_start(eval_chunk, gstate.executor.boundary_start_idx);
	WindowInputExpression boundary_end(eval_chunk, gstate.executor.boundary_end_idx);

	const auto count = eval_chunk.size();
	bounds.Reset();
	state.Bounds(bounds, row_idx, range, count, boundary_start, boundary_end, partition_mask, order_mask);
}

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
// WindowExecutor
//===--------------------------------------------------------------------===//
WindowExecutor::WindowExecutor(BoundWindowExpression &wexpr, ClientContext &context, WindowSharedExpressions &shared)
    : wexpr(wexpr), context(context),
      range_expr((WindowBoundariesState::HasPrecedingRange(wexpr) || WindowBoundariesState::HasFollowingRange(wexpr))
                     ? wexpr.orders[0].expression.get()
                     : nullptr) {
	if (range_expr) {
		range_idx = shared.RegisterCollection(wexpr.orders[0].expression, false);
	}

	boundary_start_idx = shared.RegisterEvaluate(wexpr.start_expr);
	boundary_end_idx = shared.RegisterEvaluate(wexpr.end_expr);
}

WindowExecutorGlobalState::WindowExecutorGlobalState(const WindowExecutor &executor, const idx_t payload_count,
                                                     const ValidityMask &partition_mask, const ValidityMask &order_mask)
    : executor(executor), payload_count(payload_count), partition_mask(partition_mask), order_mask(order_mask) {
	for (const auto &child : executor.wexpr.children) {
		arg_types.emplace_back(child->return_type);
	}
}

WindowExecutorLocalState::WindowExecutorLocalState(const WindowExecutorGlobalState &gstate) {
}

void WindowExecutorLocalState::Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                    idx_t input_idx) {
}

void WindowExecutorLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	const auto range_idx = gstate.executor.range_idx;
	if (range_idx != DConstants::INVALID_INDEX) {
		range_cursor = make_uniq<WindowCursor>(*collection, range_idx);
	}
}

unique_ptr<WindowExecutorGlobalState> WindowExecutor::GetGlobalState(const idx_t payload_count,
                                                                     const ValidityMask &partition_mask,
                                                                     const ValidityMask &order_mask) const {
	return make_uniq<WindowExecutorGlobalState>(*this, payload_count, partition_mask, order_mask);
}

unique_ptr<WindowExecutorLocalState> WindowExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowExecutorBoundsState>(gstate);
}

void WindowExecutor::Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
                          WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const {
	lstate.Sink(gstate, sink_chunk, coll_chunk, input_idx);
}

void WindowExecutor::Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                              CollectionPtr collection) const {
	lstate.Finalize(gstate, collection);
}

//===--------------------------------------------------------------------===//
// WindowAggregateExecutor
//===--------------------------------------------------------------------===//
class WindowAggregateExecutorGlobalState : public WindowExecutorGlobalState {
public:
	WindowAggregateExecutorGlobalState(const WindowAggregateExecutor &executor, const idx_t payload_count,
	                                   const ValidityMask &partition_mask, const ValidityMask &order_mask);

	// aggregate global state
	unique_ptr<WindowAggregatorState> gsink;

	// the filter reference expression.
	const Expression *filter_ref;
};

bool WindowAggregateExecutor::IsConstantAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}
	// window exclusion cannot be handled by constant aggregates
	if (wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}

	//	COUNT(*) is already handled efficiently by segment trees.
	if (wexpr.children.empty()) {
		return false;
	}

	/*
	    The default framing option is RANGE UNBOUNDED PRECEDING, which
	    is the same as RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
	    ROW; it sets the frame to be all rows from the partition start
	    up through the current row's last peer (a row that the window's
	    ORDER BY clause considers equivalent to the current row; all
	    rows are peers if there is no ORDER BY). In general, UNBOUNDED
	    PRECEDING means that the frame starts with the first row of the
	    partition, and similarly UNBOUNDED FOLLOWING means that the
	    frame ends with the last row of the partition, regardless of
	    RANGE, ROWS or GROUPS mode. In ROWS mode, CURRENT ROW means that
	    the frame starts or ends with the current row; but in RANGE or
	    GROUPS mode it means that the frame starts or ends with the
	    current row's first or last peer in the ORDER BY ordering. The
	    offset PRECEDING and offset FOLLOWING options vary in meaning
	    depending on the frame mode.
	*/
	switch (wexpr.start) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!wexpr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	switch (wexpr.end) {
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		if (!wexpr.orders.empty()) {
			return false;
		}
		break;
	default:
		return false;
	}

	return true;
}

bool WindowAggregateExecutor::IsDistinctAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}

	return wexpr.distinct;
}

bool WindowAggregateExecutor::IsCustomAggregate() {
	if (!wexpr.aggregate) {
		return false;
	}

	if (!AggregateObject(wexpr).function.window) {
		return false;
	}

	return (mode < WindowAggregationMode::COMBINE);
}

void WindowExecutor::Evaluate(idx_t row_idx, DataChunk &eval_chunk, Vector &result, WindowExecutorLocalState &lstate,
                              WindowExecutorGlobalState &gstate) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	lbstate.UpdateBounds(gstate, row_idx, eval_chunk, lstate.range_cursor);

	const auto count = eval_chunk.size();
	EvaluateInternal(gstate, lstate, eval_chunk, result, count, row_idx);

	result.Verify(count);
}

WindowAggregateExecutor::WindowAggregateExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared, WindowAggregationMode mode)
    : WindowExecutor(wexpr, context, shared), mode(mode) {
	auto return_type = wexpr.return_type;

	// Force naive for SEPARATE mode or for (currently!) unsupported functionality
	const auto force_naive =
	    !ClientConfig::GetConfig(context).enable_optimizer || mode == WindowAggregationMode::SEPARATE;
	if (force_naive || (wexpr.distinct && wexpr.exclude_clause != WindowExcludeMode::NO_OTHER)) {
		aggregator = make_uniq<WindowNaiveAggregator>(wexpr, wexpr.exclude_clause, shared);
	} else if (IsDistinctAggregate()) {
		// build a merge sort tree
		// see https://dl.acm.org/doi/pdf/10.1145/3514221.3526184
		aggregator = make_uniq<WindowDistinctAggregator>(wexpr, wexpr.exclude_clause, shared, context);
	} else if (IsConstantAggregate()) {
		aggregator = make_uniq<WindowConstantAggregator>(wexpr, wexpr.exclude_clause, shared);
	} else if (IsCustomAggregate()) {
		aggregator = make_uniq<WindowCustomAggregator>(wexpr, wexpr.exclude_clause, shared);
	} else {
		// build a segment tree for frame-adhering aggregates
		// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
		aggregator = make_uniq<WindowSegmentTree>(wexpr, mode, wexpr.exclude_clause, shared);
	}

	// Compute the FILTER with the other eval columns.
	// Anyone who needs it can then convert it to the form they need.
	if (wexpr.filter_expr) {
		const auto filter_idx = shared.RegisterSink(wexpr.filter_expr);
		filter_ref = make_uniq<BoundReferenceExpression>(wexpr.filter_expr->return_type, filter_idx);
	}
}

WindowAggregateExecutorGlobalState::WindowAggregateExecutorGlobalState(const WindowAggregateExecutor &executor,
                                                                       const idx_t group_count,
                                                                       const ValidityMask &partition_mask,
                                                                       const ValidityMask &order_mask)
    : WindowExecutorGlobalState(executor, group_count, partition_mask, order_mask),
      filter_ref(executor.filter_ref.get()) {
	gsink = executor.aggregator->GetGlobalState(executor.context, group_count, partition_mask);
}

unique_ptr<WindowExecutorGlobalState> WindowAggregateExecutor::GetGlobalState(const idx_t payload_count,
                                                                              const ValidityMask &partition_mask,
                                                                              const ValidityMask &order_mask) const {
	return make_uniq<WindowAggregateExecutorGlobalState>(*this, payload_count, partition_mask, order_mask);
}

class WindowAggregateExecutorLocalState : public WindowExecutorBoundsState {
public:
	WindowAggregateExecutorLocalState(const WindowExecutorGlobalState &gstate, const WindowAggregator &aggregator)
	    : WindowExecutorBoundsState(gstate), filter_executor(gstate.executor.context) {

		auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
		aggregator_state = aggregator.GetLocalState(*gastate.gsink);

		// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
		auto filter_ref = gastate.filter_ref;
		if (filter_ref) {
			filter_executor.AddExpression(*filter_ref);
			filter_sel.Initialize(STANDARD_VECTOR_SIZE);
		}
	}

public:
	// state of aggregator
	unique_ptr<WindowAggregatorState> aggregator_state;
	//! Executor for any filter clause
	ExpressionExecutor filter_executor;
	//! Result of filtering
	SelectionVector filter_sel;
};

unique_ptr<WindowExecutorLocalState>
WindowAggregateExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowAggregateExecutorLocalState>(gstate, *aggregator);
}

void WindowAggregateExecutor::Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, const idx_t input_idx,
                                   WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	auto &filter_sel = lastate.filter_sel;
	auto &filter_executor = lastate.filter_executor;

	idx_t filtered = 0;
	SelectionVector *filtering = nullptr;
	if (gastate.filter_ref) {
		filtering = &filter_sel;
		filtered = filter_executor.SelectExpression(sink_chunk, filter_sel);
	}

	D_ASSERT(aggregator);
	auto &gestate = *gastate.gsink;
	auto &lestate = *lastate.aggregator_state;
	aggregator->Sink(gestate, lestate, sink_chunk, coll_chunk, input_idx, filtering, filtered);

	WindowExecutor::Sink(sink_chunk, coll_chunk, input_idx, gstate, lstate);
}

static void ApplyWindowStats(const WindowBoundary &boundary, FrameDelta &delta, BaseStatistics *base, bool is_start) {
	// Avoid overflow by clamping to the frame bounds
	auto base_stats = delta;

	switch (boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		if (is_start) {
			delta.end = 0;
			return;
		}
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		if (!is_start) {
			delta.begin = 0;
			return;
		}
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		delta.begin = delta.end = 0;
		return;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		if (base && base->GetStatsType() == StatisticsType::NUMERIC_STATS && NumericStats::HasMinMax(*base)) {
			//	Preceding so negative offset from current row
			base_stats.begin = NumericStats::GetMin<int64_t>(*base);
			base_stats.end = NumericStats::GetMax<int64_t>(*base);
			if (delta.begin < base_stats.end && base_stats.end < delta.end) {
				delta.begin = -base_stats.end;
			}
			if (delta.begin < base_stats.begin && base_stats.begin < delta.end) {
				delta.end = -base_stats.begin + 1;
			}
		}
		return;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		if (base && base->GetStatsType() == StatisticsType::NUMERIC_STATS && NumericStats::HasMinMax(*base)) {
			base_stats.begin = NumericStats::GetMin<int64_t>(*base);
			base_stats.end = NumericStats::GetMax<int64_t>(*base);
			if (base_stats.end < delta.end) {
				delta.end = base_stats.end + 1;
			}
		}
		return;

	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return;
	default:
		break;
	}

	if (is_start) {
		throw InternalException("Unsupported window start boundary");
	} else {
		throw InternalException("Unsupported window end boundary");
	}
}

void WindowAggregateExecutor::Finalize(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                       CollectionPtr collection) const {
	WindowExecutor::Finalize(gstate, lstate, collection);

	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &gsink = gastate.gsink;
	D_ASSERT(aggregator);

	//	Estimate the frame statistics
	//	Default to the entire partition if we don't know anything
	FrameStats stats;
	const auto count = NumericCast<int64_t>(gastate.payload_count);

	//	First entry is the frame start
	stats[0] = FrameDelta(-count, count);
	auto base = wexpr.expr_stats.empty() ? nullptr : wexpr.expr_stats[0].get();
	ApplyWindowStats(wexpr.start, stats[0], base, true);

	//	Second entry is the frame end
	stats[1] = FrameDelta(-count, count);
	base = wexpr.expr_stats.empty() ? nullptr : wexpr.expr_stats[1].get();
	ApplyWindowStats(wexpr.end, stats[1], base, false);

	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	aggregator->Finalize(*gsink, *lastate.aggregator_state, collection, stats);
}

void WindowAggregateExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &gastate = gstate.Cast<WindowAggregateExecutorGlobalState>();
	auto &lastate = lstate.Cast<WindowAggregateExecutorLocalState>();
	auto &gsink = gastate.gsink;
	D_ASSERT(aggregator);

	auto &agg_state = *lastate.aggregator_state;

	aggregator->Evaluate(*gsink, agg_state, lastate.bounds, result, count, row_idx);
}

//===--------------------------------------------------------------------===//
// WindowRowNumberExecutor
//===--------------------------------------------------------------------===//
WindowRowNumberExecutor::WindowRowNumberExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

void WindowRowNumberExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = NumericCast<int64_t>(row_idx - partition_begin[i] + 1);
	}
}

//===--------------------------------------------------------------------===//
// WindowPeerState
//===--------------------------------------------------------------------===//
class WindowPeerState : public WindowExecutorBoundsState {
public:
	explicit WindowPeerState(const WindowExecutorGlobalState &gstate) : WindowExecutorBoundsState(gstate) {
	}

public:
	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);
};

void WindowPeerState::NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx) {
	if (partition_begin == row_idx) {
		dense_rank = 1;
		rank = 1;
		rank_equal = 0;
	} else if (peer_begin == row_idx) {
		dense_rank++;
		rank += rank_equal;
		rank_equal = 0;
	}
	rank_equal++;
}

WindowRankExecutor::WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                       WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState> WindowRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                          DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.rank);
	}
}

WindowDenseRankExecutor::WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowDenseRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowDenseRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();

	auto &order_mask = gstate.order_mask;
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	//	The previous dense rank is the number of order mask bits in [partition_begin, row_idx)
	lpeer.dense_rank = 0;

	auto order_begin = partition_begin[0];
	idx_t begin_idx;
	idx_t begin_offset;
	order_mask.GetEntryIndex(order_begin, begin_idx, begin_offset);

	auto order_end = row_idx;
	idx_t end_idx;
	idx_t end_offset;
	order_mask.GetEntryIndex(order_end, end_idx, end_offset);

	//	If they are in the same entry, just loop
	if (begin_idx == end_idx) {
		const auto entry = order_mask.GetValidityEntry(begin_idx);
		for (; begin_offset < end_offset; ++begin_offset) {
			lpeer.dense_rank += order_mask.RowIsValid(entry, begin_offset);
		}
	} else {
		// Count the ragged bits at the start of the partition
		if (begin_offset) {
			const auto entry = order_mask.GetValidityEntry(begin_idx);
			for (; begin_offset < order_mask.BITS_PER_VALUE; ++begin_offset) {
				lpeer.dense_rank += order_mask.RowIsValid(entry, begin_offset);
				++order_begin;
			}
			++begin_idx;
		}

		//	Count the the aligned bits.
		ValidityMask tail_mask(order_mask.GetData() + begin_idx, end_idx - begin_idx);
		lpeer.dense_rank += tail_mask.CountValid(order_end - order_begin);
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.dense_rank);
	}
}

WindowPercentRankExecutor::WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                     WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowPercentRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowPercentRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                 DataChunk &eval_chunk, Vector &result, idx_t count,
                                                 idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<double>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i] - 1));
		double percent_rank = denom > 0 ? ((double)lpeer.rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

//===--------------------------------------------------------------------===//
// WindowCumeDistExecutor
//===--------------------------------------------------------------------===//
WindowCumeDistExecutor::WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                               WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

void WindowCumeDistExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lbstate = lstate.Cast<WindowExecutorBoundsState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(lbstate.bounds.data[PEER_END]);
	auto rdata = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i]));
		double cume_dist = denom > 0 ? ((double)(peer_end[i] - partition_begin[i])) / denom : 0;
		rdata[i] = cume_dist;
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
		if (child_idx != DConstants::INVALID_INDEX && executor.wexpr.ignore_nulls) {
			lock_guard<mutex> ignore_nulls_guard(lock);
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
	}

	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	//! The corresponding global value state
	const WindowValueGlobalState &gvstate;
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
		//	Same as NTH_VALUE(..., 1)
		idx_t n = 1;
		const auto first_idx =
		    WindowBoundariesState::FindNextStart(*lvstate.ignore_nulls_exclude, window_begin[i], window_end[i], n);
		if (!n) {
			cursor.CopyCell(0, first_idx, result, i);
		} else {
			FlatVector::SetNull(result, i, true);
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
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
	auto &lvstate = lstate.Cast<WindowValueLocalState>();
	auto &cursor = *lvstate.cursor;
	D_ASSERT(cursor.chunk.ColumnCount() == 1);
	auto window_begin = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(lvstate.bounds.data[FRAME_END]);
	WindowInputExpression nth_col(eval_chunk, nth_idx);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ApplyExclusion(lvstate.bounds, row_idx, i);
		}

		if (window_begin[i] >= window_end[i]) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
		// returns NULL if there is no such row.
		if (nth_col.CellIsNull(row_idx)) {
			FlatVector::SetNull(result, i, true);
		} else {
			auto n_param = nth_col.GetCell<int64_t>(row_idx);
			if (n_param < 1) {
				FlatVector::SetNull(result, i, true);
			} else {
				auto n = idx_t(n_param);
				const auto nth_index = WindowBoundariesState::FindNextStart(*lvstate.ignore_nulls_exclude,
				                                                            window_begin[i], window_end[i], n);
				if (!n) {
					cursor.CopyCell(0, nth_index, result, i);
				} else {
					FlatVector::SetNull(result, i, true);
				}
			}
		}

		if (lvstate.exclusion_filter) {
			lvstate.exclusion_filter->ResetMask(row_idx, i);
		}
	}
}

} // namespace duckdb
