#include "duckdb/function/window/window_aggregate_function.hpp"

#include "duckdb/function/window/window_constant_aggregator.hpp"
#include "duckdb/function/window/window_custom_aggregator.hpp"
#include "duckdb/function/window/window_distinct_aggregator.hpp"
#include "duckdb/function/window/window_naive_aggregator.hpp"
#include "duckdb/function/window/window_segment_tree.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

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

} // namespace duckdb
