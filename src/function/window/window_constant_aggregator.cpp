#include "duckdb/function/window/window_constant_aggregator.hpp"

#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/window/window_aggregate_states.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowConstantAggregatorGlobalState
//===--------------------------------------------------------------------===//

class WindowConstantAggregatorGlobalState : public WindowAggregatorGlobalState {
public:
	WindowConstantAggregatorGlobalState(ClientContext &context, const WindowConstantAggregator &aggregator, idx_t count,
	                                    const ValidityMask &partition_mask);

	void Finalize(const FrameStats &stats);

	~WindowConstantAggregatorGlobalState() override {
		statef.Destroy();
	}

	//! Partition starts
	vector<idx_t> partition_offsets;
	//! Reused result state container for the window functions
	WindowAggregateStates statef;
	//! Aggregate results
	unique_ptr<Vector> results;
};

WindowConstantAggregatorGlobalState::WindowConstantAggregatorGlobalState(ClientContext &context,
                                                                         const WindowConstantAggregator &aggregator,
                                                                         idx_t group_count,
                                                                         const ValidityMask &partition_mask)
    : WindowAggregatorGlobalState(context, aggregator, STANDARD_VECTOR_SIZE), statef(aggr) {

	// Locate the partition boundaries
	if (partition_mask.AllValid()) {
		partition_offsets.emplace_back(0);
	} else {
		idx_t entry_idx;
		idx_t shift;
		for (idx_t start = 0; start < group_count;) {
			partition_mask.GetEntryIndex(start, entry_idx, shift);

			//	If start is aligned with the start of a block,
			//	and the block is blank, then skip forward one block.
			const auto block = partition_mask.GetValidityEntry(entry_idx);
			if (partition_mask.NoneValid(block) && !shift) {
				start += ValidityMask::BITS_PER_VALUE;
				continue;
			}

			// Loop over the block
			for (; shift < ValidityMask::BITS_PER_VALUE && start < group_count; ++shift, ++start) {
				if (partition_mask.RowIsValid(block, shift)) {
					partition_offsets.emplace_back(start);
				}
			}
		}
	}

	//	Initialise the vector for caching the results
	results = make_uniq<Vector>(aggregator.result_type, partition_offsets.size());

	//	Initialise the final states
	statef.Initialize(partition_offsets.size());

	// Add final guard
	partition_offsets.emplace_back(group_count);
}

//===--------------------------------------------------------------------===//
// WindowConstantAggregatorLocalState
//===--------------------------------------------------------------------===//
class WindowConstantAggregatorLocalState : public WindowAggregatorLocalState {
public:
	explicit WindowConstantAggregatorLocalState(const WindowConstantAggregatorGlobalState &gstate);
	~WindowConstantAggregatorLocalState() override {
	}

	void Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel,
	          idx_t filtered);
	void Combine(WindowConstantAggregatorGlobalState &gstate);

public:
	//! The global state we are sharing
	const WindowConstantAggregatorGlobalState &gstate;
	//! Reusable chunk for sinking
	DataChunk inputs;
	//! Chunk for referencing the input columns
	DataChunk payload_chunk;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! Reused result state container for the window functions
	WindowAggregateStates statef;
	//! The current result partition being read
	idx_t partition;
	//! Shared SV for evaluation
	SelectionVector matches;
};

WindowConstantAggregatorLocalState::WindowConstantAggregatorLocalState(
    const WindowConstantAggregatorGlobalState &gstate)
    : gstate(gstate), statep(Value::POINTER(0)), statef(gstate.statef.aggr), partition(0) {
	matches.Initialize();

	//	Start the aggregates
	auto &partition_offsets = gstate.partition_offsets;
	auto &aggregator = gstate.aggregator;
	statef.Initialize(partition_offsets.size() - 1);

	// Set up shared buffer
	inputs.Initialize(Allocator::DefaultAllocator(), aggregator.arg_types);
	payload_chunk.InitializeEmpty(inputs.GetTypes());

	gstate.locals++;
}

//===--------------------------------------------------------------------===//
// WindowConstantAggregator
//===--------------------------------------------------------------------===//
bool WindowConstantAggregator::CanAggregate(const BoundWindowExpression &wexpr) {
	if (!wexpr.aggregate) {
		return false;
	}
	// window exclusion cannot be handled by constant aggregates
	if (wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}

	// 	DISTINCT aggregation cannot be handled by constant aggregation
	if (wexpr.distinct) {
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

BoundWindowExpression &WindowConstantAggregator::RebindAggregate(ClientContext &context, BoundWindowExpression &wexpr) {
	FunctionBinder::BindSortedAggregate(context, wexpr);

	return wexpr;
}

WindowConstantAggregator::WindowConstantAggregator(BoundWindowExpression &wexpr, WindowSharedExpressions &shared,
                                                   ClientContext &context)
    : WindowAggregator(RebindAggregate(context, wexpr)) {

	// We only need these values for Sink
	for (auto &child : wexpr.children) {
		child_idx.emplace_back(shared.RegisterSink(child));
	}
}

unique_ptr<WindowAggregatorState> WindowConstantAggregator::GetGlobalState(ClientContext &context, idx_t group_count,
                                                                           const ValidityMask &partition_mask) const {
	return make_uniq<WindowConstantAggregatorGlobalState>(context, *this, group_count, partition_mask);
}

void WindowConstantAggregator::Sink(WindowAggregatorState &gsink, WindowAggregatorState &lstate, DataChunk &sink_chunk,
                                    DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel,
                                    idx_t filtered) {
	auto &lastate = lstate.Cast<WindowConstantAggregatorLocalState>();

	lastate.Sink(sink_chunk, coll_chunk, input_idx, filter_sel, filtered);
}

void WindowConstantAggregatorLocalState::Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t row,
                                              optional_ptr<SelectionVector> filter_sel, idx_t filtered) {
	auto &partition_offsets = gstate.partition_offsets;
	const auto &aggr = gstate.aggr;
	const auto chunk_begin = row;
	const auto chunk_end = chunk_begin + sink_chunk.size();
	idx_t partition =
	    idx_t(std::upper_bound(partition_offsets.begin(), partition_offsets.end(), row) - partition_offsets.begin()) -
	    1;

	auto state_f_data = statef.GetData();
	auto state_p_data = FlatVector::GetData<data_ptr_t>(statep);

	auto &child_idx = gstate.aggregator.child_idx;
	for (column_t c = 0; c < child_idx.size(); ++c) {
		payload_chunk.data[c].Reference(sink_chunk.data[child_idx[c]]);
	}

	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	idx_t begin = 0;
	idx_t filter_idx = 0;
	auto partition_end = partition_offsets[partition + 1];
	while (row < chunk_end) {
		if (row == partition_end) {
			++partition;
			partition_end = partition_offsets[partition + 1];
		}
		partition_end = MinValue(partition_end, chunk_end);
		auto end = partition_end - chunk_begin;

		inputs.Reset();
		if (filter_sel) {
			// 	Slice to any filtered rows in [begin, end)
			SelectionVector sel;

			//	Find the first value in [begin, end)
			for (; filter_idx < filtered; ++filter_idx) {
				auto idx = filter_sel->get_index(filter_idx);
				if (idx >= begin) {
					break;
				}
			}

			//	Find the first value in [end, filtered)
			sel.Initialize(filter_sel->data() + filter_idx);
			idx_t nsel = 0;
			for (; filter_idx < filtered; ++filter_idx, ++nsel) {
				auto idx = filter_sel->get_index(filter_idx);
				if (idx >= end) {
					break;
				}
			}

			if (nsel != inputs.size()) {
				inputs.Slice(payload_chunk, sel, nsel);
			}
		} else {
			//	Slice to [begin, end)
			if (begin) {
				for (idx_t c = 0; c < payload_chunk.ColumnCount(); ++c) {
					inputs.data[c].Slice(payload_chunk.data[c], begin, end);
				}
			} else {
				inputs.Reference(payload_chunk);
			}
			inputs.SetCardinality(end - begin);
		}

		//	Aggregate the filtered rows into a single state
		const auto count = inputs.size();
		auto state = state_f_data[partition];
		if (aggr.function.simple_update) {
			aggr.function.simple_update(inputs.data.data(), aggr_input_data, inputs.ColumnCount(), state, count);
		} else {
			state_p_data[0] = state_f_data[partition];
			aggr.function.update(inputs.data.data(), aggr_input_data, inputs.ColumnCount(), statep, count);
		}

		//	Skip filtered rows too!
		row += end - begin;
		begin = end;
	}
}

void WindowConstantAggregator::Finalize(WindowAggregatorState &gstate, WindowAggregatorState &lstate,
                                        CollectionPtr collection, const FrameStats &stats) {
	auto &gastate = gstate.Cast<WindowConstantAggregatorGlobalState>();
	auto &lastate = lstate.Cast<WindowConstantAggregatorLocalState>();

	//	Single-threaded combine
	lock_guard<mutex> finalize_guard(gastate.lock);
	lastate.statef.Combine(gastate.statef);
	lastate.statef.Destroy();

	gastate.statef.Finalize(*gastate.results);
}

unique_ptr<WindowAggregatorState> WindowConstantAggregator::GetLocalState(const WindowAggregatorState &gstate) const {
	return make_uniq<WindowConstantAggregatorLocalState>(gstate.Cast<WindowConstantAggregatorGlobalState>());
}

void WindowConstantAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                        const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gasink = gsink.Cast<WindowConstantAggregatorGlobalState>();
	const auto &partition_offsets = gasink.partition_offsets;
	const auto &results = *gasink.results;

	auto begins = FlatVector::GetData<const idx_t>(bounds.data[FRAME_BEGIN]);
	//	Chunk up the constants and copy them one at a time
	auto &lcstate = lstate.Cast<WindowConstantAggregatorLocalState>();
	idx_t matched = 0;
	idx_t target_offset = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto begin = begins[i];
		//	Find the partition containing [begin, end)
		while (partition_offsets[lcstate.partition + 1] <= begin) {
			//	Flush the previous partition's data
			if (matched) {
				VectorOperations::Copy(results, result, lcstate.matches, matched, 0, target_offset);
				target_offset += matched;
				matched = 0;
			}
			++lcstate.partition;
		}

		lcstate.matches.set_index(matched++, lcstate.partition);
	}

	//	Flush the last partition
	if (matched) {
		// Optimize constant result
		if (target_offset == 0 && matched == count) {
			VectorOperations::Copy(results, result, lcstate.matches, 1, 0, target_offset);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		} else {
			VectorOperations::Copy(results, result, lcstate.matches, matched, 0, target_offset);
		}
	}
}

} // namespace duckdb
