#include "duckdb/execution/window_segment_tree.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/execution/window_executor.hpp"

#include <numeric>
#include <utility>

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowAggregator
//===--------------------------------------------------------------------===//
WindowAggregatorState::WindowAggregatorState() : allocator(Allocator::DefaultAllocator()) {
}

class WindowAggregatorGlobalState : public WindowAggregatorState {
public:
	WindowAggregatorGlobalState(const WindowAggregator &aggregator_p, idx_t group_count)
	    : aggregator(aggregator_p), filter_pos(0) {

		if (!aggregator.arg_types.empty()) {
			inputs.Initialize(Allocator::DefaultAllocator(), aggregator.arg_types);
		}
		if (aggregator.aggr.filter) {
			// 	Start with all invalid and set the ones that pass
			filter_bits.resize(ValidityMask::ValidityMaskSize(group_count), 0);
			filter_mask.Initialize(filter_bits.data());
		}
	}

	//! The aggregator data
	const WindowAggregator &aggregator;

	//! Partition data chunk
	DataChunk inputs;

	//! The filtered rows in inputs.
	vector<validity_t> filter_bits;
	ValidityMask filter_mask;
	idx_t filter_pos;
};

WindowAggregator::WindowAggregator(AggregateObject aggr_p, const vector<LogicalType> &arg_types_p,
                                   const LogicalType &result_type_p, const WindowExcludeMode exclude_mode_p)
    : aggr(std::move(aggr_p)), arg_types(arg_types_p), result_type(result_type_p),
      state_size(aggr.function.state_size()), exclude_mode(exclude_mode_p) {
}

WindowAggregator::~WindowAggregator() {
}

unique_ptr<WindowAggregatorState> WindowAggregator::GetGlobalState(idx_t group_count, const ValidityMask &) const {
	return make_uniq<WindowAggregatorGlobalState>(*this, group_count);
}

void WindowAggregator::Sink(WindowAggregatorState &gsink, DataChunk &payload_chunk, SelectionVector *filter_sel,
                            idx_t filtered) {
	auto &gasink = gsink.Cast<WindowAggregatorGlobalState>();
	auto &inputs = gasink.inputs;
	auto &filter_mask = gasink.filter_mask;
	auto &filter_pos = gasink.filter_pos;
	if (inputs.ColumnCount()) {
		inputs.Append(payload_chunk, true);
	}
	if (filter_sel) {
		for (idx_t f = 0; f < filtered; ++f) {
			filter_mask.SetValid(filter_pos + filter_sel->get_index(f));
		}
		filter_pos += payload_chunk.size();
	}
}

void WindowAggregator::Finalize(WindowAggregatorState &gstate, const FrameStats &stats) {
}

//===--------------------------------------------------------------------===//
// WindowConstantAggregator
//===--------------------------------------------------------------------===//
class WindowConstantAggregatorGlobalState : public WindowAggregatorGlobalState {
public:
	WindowConstantAggregatorGlobalState(const WindowConstantAggregator &aggregator, idx_t count,
	                                    const ValidityMask &partition_mask);

	void Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered);
	void Finalize(const FrameStats &stats);

	void AggregateInit();
	void AggegateFinal(Vector &result, idx_t rid);

	//! Partition starts
	vector<idx_t> partition_offsets;
	//! Aggregate results
	unique_ptr<Vector> results;
	//! The current result partition being built/read
	idx_t partition;
	//! The current input row being built/read
	idx_t row;
	//! Data pointer that contains a single state, used for intermediate window segment aggregation
	vector<data_t> state;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! Reused result state container for the window functions
	Vector statef;
};

WindowConstantAggregatorGlobalState::WindowConstantAggregatorGlobalState(const WindowConstantAggregator &aggregator,
                                                                         idx_t group_count,
                                                                         const ValidityMask &partition_mask)
    : WindowAggregatorGlobalState(aggregator, 0), partition(0), row(0), state(aggregator.state_size),
      statep(Value::POINTER(CastPointerToValue(state.data()))),
      statef(Value::POINTER(CastPointerToValue(state.data()))) {

	statef.SetVectorType(VectorType::FLAT_VECTOR); // Prevent conversion of results to constants

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
	partition_offsets.emplace_back(group_count);

	//	Start the first aggregate
	AggregateInit();
}

WindowConstantAggregator::WindowConstantAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types,
                                                   const LogicalType &result_type,
                                                   const WindowExcludeMode exclude_mode_p)
    : WindowAggregator(std::move(aggr), arg_types, result_type, exclude_mode_p) {
}

void WindowConstantAggregatorGlobalState::AggregateInit() {
	const auto &aggr = aggregator.aggr;
	aggr.function.initialize(state.data());
}

void WindowConstantAggregatorGlobalState::AggegateFinal(Vector &result, idx_t rid) {
	const auto &aggr = aggregator.aggr;
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.finalize(statef, aggr_input_data, result, 1, rid);

	if (aggr.function.destructor) {
		aggr.function.destructor(statef, aggr_input_data, 1);
	}
}

unique_ptr<WindowAggregatorState> WindowConstantAggregator::GetGlobalState(idx_t group_count,
                                                                           const ValidityMask &partition_mask) const {
	return make_uniq<WindowConstantAggregatorGlobalState>(*this, group_count, partition_mask);
}

void WindowConstantAggregator::Sink(WindowAggregatorState &gsink, DataChunk &payload_chunk, SelectionVector *filter_sel,
                                    idx_t filtered) {
	auto &gasink = gsink.Cast<WindowConstantAggregatorGlobalState>();

	gasink.Sink(payload_chunk, filter_sel, filtered);
}

void WindowConstantAggregatorGlobalState::Sink(DataChunk &payload_chunk, SelectionVector *filter_sel, idx_t filtered) {
	const auto &aggr = aggregator.aggr;
	const auto chunk_begin = row;
	const auto chunk_end = chunk_begin + payload_chunk.size();

	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	idx_t begin = 0;
	idx_t filter_idx = 0;
	auto partition_end = partition_offsets[partition + 1];
	while (row < chunk_end) {
		if (row == partition_end) {
			AggegateFinal(*results, partition++);
			AggregateInit();
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
		if (aggr.function.simple_update) {
			aggr.function.simple_update(inputs.data.data(), aggr_input_data, inputs.ColumnCount(), state.data(), count);
		} else {
			aggr.function.update(inputs.data.data(), aggr_input_data, inputs.ColumnCount(), statep, count);
		}

		//	Skip filtered rows too!
		row += end - begin;
		begin = end;
	}
}

void WindowConstantAggregatorGlobalState::Finalize(const FrameStats &stats) {
	AggegateFinal(*results, partition++);
}

void WindowConstantAggregator::Finalize(WindowAggregatorState &gsink, const FrameStats &stats) {
	auto &gasink = gsink.Cast<WindowConstantAggregatorGlobalState>();

	gasink.Finalize(stats);
}

class WindowConstantAggregatorState : public WindowAggregatorState {
public:
	WindowConstantAggregatorState() : partition(0) {
		matches.Initialize();
	}
	~WindowConstantAggregatorState() override {
	}

public:
	//! The current result partition being read
	idx_t partition;
	//! Shared SV for evaluation
	SelectionVector matches;
};

unique_ptr<WindowAggregatorState> WindowConstantAggregator::GetLocalState() const {
	return make_uniq<WindowConstantAggregatorState>();
}

void WindowConstantAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                        const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gasink = gsink.Cast<WindowConstantAggregatorGlobalState>();
	const auto &partition_offsets = gasink.partition_offsets;
	const auto &results = *gasink.results;

	auto begins = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	//	Chunk up the constants and copy them one at a time
	auto &lcstate = lstate.Cast<WindowConstantAggregatorState>();
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
		VectorOperations::Copy(results, result, lcstate.matches, matched, 0, target_offset);
	}
}

//===--------------------------------------------------------------------===//
// WindowCustomAggregator
//===--------------------------------------------------------------------===//
WindowCustomAggregator::WindowCustomAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types,
                                               const LogicalType &result_type, const WindowExcludeMode exclude_mode)
    : WindowAggregator(std::move(aggr), arg_types, result_type, exclude_mode) {
}

WindowCustomAggregator::~WindowCustomAggregator() {
}

class WindowCustomAggregatorState : public WindowAggregatorState {
public:
	WindowCustomAggregatorState(const AggregateObject &aggr, const WindowExcludeMode exclude_mode);
	~WindowCustomAggregatorState() override;

public:
	//! The aggregate function
	const AggregateObject &aggr;
	//! Data pointer that contains a single state, shared by all the custom evaluators
	vector<data_t> state;
	//! Reused result state container for the window functions
	Vector statef;
	//! The frame boundaries, used for the window functions
	SubFrames frames;
};

static void InitSubFrames(SubFrames &frames, const WindowExcludeMode exclude_mode) {
	idx_t nframes = 0;
	switch (exclude_mode) {
	case WindowExcludeMode::NO_OTHER:
		nframes = 1;
		break;
	case WindowExcludeMode::TIES:
		nframes = 3;
		break;
	case WindowExcludeMode::CURRENT_ROW:
	case WindowExcludeMode::GROUP:
		nframes = 2;
		break;
	}
	frames.resize(nframes, {0, 0});
}

class WindowCustomAggregatorGlobalState : public WindowAggregatorGlobalState {
public:
	explicit WindowCustomAggregatorGlobalState(const WindowCustomAggregator &aggregator, idx_t group_count)
	    : WindowAggregatorGlobalState(aggregator, group_count) {

		gcstate = make_uniq<WindowCustomAggregatorState>(aggregator.aggr, aggregator.exclude_mode);
	}

	//! Data pointer that contains a single local state, used for global custom window execution state
	unique_ptr<WindowCustomAggregatorState> gcstate;
	//! Partition description for custom window APIs
	unique_ptr<WindowPartitionInput> partition_input;
};

WindowCustomAggregatorState::WindowCustomAggregatorState(const AggregateObject &aggr,
                                                         const WindowExcludeMode exclude_mode)
    : aggr(aggr), state(aggr.function.state_size()), statef(Value::POINTER(CastPointerToValue(state.data()))),
      frames(3, {0, 0}) {
	// if we have a frame-by-frame method, share the single state
	aggr.function.initialize(state.data());

	InitSubFrames(frames, exclude_mode);
}

WindowCustomAggregatorState::~WindowCustomAggregatorState() {
	if (aggr.function.destructor) {
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
		aggr.function.destructor(statef, aggr_input_data, 1);
	}
}

unique_ptr<WindowAggregatorState> WindowCustomAggregator::GetGlobalState(idx_t group_count,
                                                                         const ValidityMask &) const {
	return make_uniq<WindowCustomAggregatorGlobalState>(*this, group_count);
}

void WindowCustomAggregator::Finalize(WindowAggregatorState &gsink, const FrameStats &stats) {
	WindowAggregator::Finalize(gsink, stats);

	auto &gcsink = gsink.Cast<WindowCustomAggregatorGlobalState>();
	auto &inputs = gcsink.inputs;
	auto &filter_mask = gcsink.filter_mask;
	gcsink.partition_input =
	    make_uniq<WindowPartitionInput>(inputs.data.data(), inputs.ColumnCount(), inputs.size(), filter_mask, stats);

	if (aggr.function.window_init) {
		auto &gcstate = *gcsink.gcstate;

		AggregateInputData aggr_input_data(aggr.GetFunctionData(), gcstate.allocator);
		aggr.function.window_init(aggr_input_data, *gcsink.partition_input, gcstate.state.data());
	}
}

unique_ptr<WindowAggregatorState> WindowCustomAggregator::GetLocalState() const {
	return make_uniq<WindowCustomAggregatorState>(aggr, exclude_mode);
}

template <typename OP>
static void EvaluateSubFrames(const DataChunk &bounds, const WindowExcludeMode exclude_mode, idx_t count, idx_t row_idx,
                              SubFrames &frames, OP operation) {
	auto begins = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto ends = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto peer_end = FlatVector::GetData<const idx_t>(bounds.data[PEER_END]);

	for (idx_t i = 0, cur_row = row_idx; i < count; ++i, ++cur_row) {
		idx_t nframes = 0;
		if (exclude_mode == WindowExcludeMode::NO_OTHER) {
			auto begin = begins[i];
			auto end = ends[i];
			frames[nframes++] = FrameBounds(begin, end);
		} else {
			//	The frame_exclusion option allows rows around the current row to be excluded from the frame,
			//	even if they would be included according to the frame start and frame end options.
			//	EXCLUDE CURRENT ROW excludes the current row from the frame.
			//	EXCLUDE GROUP excludes the current row and its ordering peers from the frame.
			//	EXCLUDE TIES excludes any peers of the current row from the frame, but not the current row itself.
			//	EXCLUDE NO OTHERS simply specifies explicitly the default behavior
			//	of not excluding the current row or its peers.
			//	https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
			//
			//	For the sake of the client, we make some guarantees about the subframes:
			//	* They are in order left-to-right
			//	* They do not intersect
			//	* start <= end
			//	* The number is always the same
			//
			//	Since we always have peer_begin <= cur_row < cur_row + 1 <= peer_end
			//	this is not too hard to arrange, but it may be that some subframes are contiguous,
			//	and some are empty.

			//	WindowExcludePart::LEFT
			auto begin = begins[i];
			auto end = (exclude_mode == WindowExcludeMode::CURRENT_ROW) ? cur_row : peer_begin[i];
			end = MaxValue(begin, end);
			frames[nframes++] = FrameBounds(begin, end);

			// with EXCLUDE TIES, in addition to the frame part right of the peer group's end,
			// we also need to consider the current row
			if (exclude_mode == WindowExcludeMode::TIES) {
				frames[nframes++] = FrameBounds(cur_row, cur_row + 1);
			}

			//	WindowExcludePart::RIGHT
			end = ends[i];
			begin = (exclude_mode == WindowExcludeMode::CURRENT_ROW) ? (cur_row + 1) : peer_end[i];
			begin = MinValue(begin, end);
			frames[nframes++] = FrameBounds(begin, end);
		}

		operation(i);
	}
}

void WindowCustomAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                      const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lcstate = lstate.Cast<WindowCustomAggregatorState>();
	auto &frames = lcstate.frames;
	const_data_ptr_t gstate_p = nullptr;
	auto &gcsink = gsink.Cast<WindowCustomAggregatorGlobalState>();
	if (gcsink.gcstate) {
		gstate_p = gcsink.gcstate->state.data();
	}

	EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t i) {
		// Extract the range
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), lstate.allocator);
		aggr.function.window(aggr_input_data, *gcsink.partition_input, gstate_p, lcstate.state.data(), frames, result,
		                     i);
	});
}

//===--------------------------------------------------------------------===//
// WindowNaiveAggregator
//===--------------------------------------------------------------------===//
WindowNaiveAggregator::WindowNaiveAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types,
                                             const LogicalType &result_type, const WindowExcludeMode exclude_mode)
    : WindowAggregator(std::move(aggr), arg_types, result_type, exclude_mode) {
}

WindowNaiveAggregator::~WindowNaiveAggregator() {
}

class WindowNaiveState : public WindowAggregatorState {
public:
	struct HashRow {
		HashRow(WindowNaiveState &state, const DataChunk &inputs) : state(state), inputs(inputs) {
		}

		size_t operator()(const idx_t &i) const {
			return state.Hash(inputs, i);
		}

		WindowNaiveState &state;
		const DataChunk &inputs;
	};

	struct EqualRow {
		EqualRow(WindowNaiveState &state, const DataChunk &inputs) : state(state), inputs(inputs) {
		}

		bool operator()(const idx_t &lhs, const idx_t &rhs) const {
			return state.KeyEqual(inputs, lhs, rhs);
		}

		WindowNaiveState &state;
		const DataChunk &inputs;
	};

	using RowSet = std::unordered_set<idx_t, HashRow, EqualRow>;

	explicit WindowNaiveState(const WindowNaiveAggregator &gsink);

	void Evaluate(const WindowAggregatorGlobalState &gsink, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx);

protected:
	//! Flush the accumulated intermediate states into the result states
	void FlushStates(const WindowAggregatorGlobalState &gsink);

	//! Hashes a value for the hash table
	size_t Hash(const DataChunk &inputs, idx_t rid);
	//! Compares two values for the hash table
	bool KeyEqual(const DataChunk &inputs, const idx_t &lhs, const idx_t &rhs);

	//! The global state
	const WindowNaiveAggregator &aggregator;
	//! Data pointer that contains a vector of states, used for row aggregation
	vector<data_t> state;
	//! Reused result state container for the aggregate
	Vector statef;
	//! A vector of pointers to "state", used for buffering intermediate aggregates
	Vector statep;
	//! Input data chunk, used for leaf segment aggregation
	DataChunk leaves;
	//! The rows beging updated.
	SelectionVector update_sel;
	//! Count of buffered values
	idx_t flush_count;
	//! The frame boundaries, used for EXCLUDE
	SubFrames frames;
	//! The optional hash table used for DISTINCT
	Vector hashes;
};

WindowNaiveState::WindowNaiveState(const WindowNaiveAggregator &aggregator_p)
    : aggregator(aggregator_p), state(aggregator.state_size * STANDARD_VECTOR_SIZE), statef(LogicalType::POINTER),
      statep((LogicalType::POINTER)), flush_count(0), hashes(LogicalType::HASH) {
	InitSubFrames(frames, aggregator.exclude_mode);

	update_sel.Initialize();

	//	Build the finalise vector that just points to the result states
	data_ptr_t state_ptr = state.data();
	D_ASSERT(statef.GetVectorType() == VectorType::FLAT_VECTOR);
	statef.SetVectorType(VectorType::CONSTANT_VECTOR);
	statef.Flatten(STANDARD_VECTOR_SIZE);
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		fdata[i] = state_ptr;
		state_ptr += aggregator.state_size;
	}
}

void WindowNaiveState::FlushStates(const WindowAggregatorGlobalState &gsink) {
	if (!flush_count) {
		return;
	}

	auto &inputs = gsink.inputs;
	leaves.Slice(inputs, update_sel, flush_count);

	auto &aggr = aggregator.aggr;
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.update(leaves.data.data(), aggr_input_data, leaves.ColumnCount(), statep, flush_count);

	flush_count = 0;
}

size_t WindowNaiveState::Hash(const DataChunk &inputs, idx_t rid) {
	auto s = UnsafeNumericCast<sel_t>(rid);
	SelectionVector sel(&s);
	leaves.Slice(inputs, sel, 1);
	leaves.Hash(hashes);

	return *FlatVector::GetData<hash_t>(hashes);
}

bool WindowNaiveState::KeyEqual(const DataChunk &inputs, const idx_t &lhs, const idx_t &rhs) {
	auto l = UnsafeNumericCast<sel_t>(lhs);
	SelectionVector lsel(&l);

	auto r = UnsafeNumericCast<sel_t>(rhs);
	SelectionVector rsel(&r);

	sel_t f = 0;
	SelectionVector fsel(&f);

	for (auto &input : inputs.data) {
		Vector left(input, lsel, 1);
		Vector right(input, rsel, 1);
		if (!VectorOperations::NotDistinctFrom(left, right, nullptr, 1, nullptr, &fsel)) {
			return false;
		}
	}

	return true;
}

void WindowNaiveState::Evaluate(const WindowAggregatorGlobalState &gsink, const DataChunk &bounds, Vector &result,
                                idx_t count, idx_t row_idx) {
	auto &aggr = aggregator.aggr;
	auto &filter_mask = gsink.filter_mask;
	auto &inputs = gsink.inputs;

	if (leaves.ColumnCount() == 0 && inputs.ColumnCount() > 0) {
		leaves.Initialize(Allocator::DefaultAllocator(), inputs.GetTypes());
	}

	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	auto pdata = FlatVector::GetData<data_ptr_t>(statep);

	HashRow hash_row(*this, inputs);
	EqualRow equal_row(*this, inputs);
	RowSet row_set(STANDARD_VECTOR_SIZE, hash_row, equal_row);

	EvaluateSubFrames(bounds, aggregator.exclude_mode, count, row_idx, frames, [&](idx_t rid) {
		auto agg_state = fdata[rid];
		aggr.function.initialize(agg_state);

		//	Just update the aggregate with the unfiltered input rows
		row_set.clear();
		for (const auto &frame : frames) {
			for (auto f = frame.start; f < frame.end; ++f) {
				if (!filter_mask.RowIsValid(f)) {
					continue;
				}

				//	Filter out duplicates
				if (aggr.IsDistinct() && !row_set.insert(f).second) {
					continue;
				}

				pdata[flush_count] = agg_state;
				update_sel[flush_count++] = UnsafeNumericCast<sel_t>(f);
				if (flush_count >= STANDARD_VECTOR_SIZE) {
					FlushStates(gsink);
				}
			}
		}
	});

	//	Flush the final states
	FlushStates(gsink);

	//	Finalise the result aggregates and write to the result
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.finalize(statef, aggr_input_data, result, count, 0);

	//	Destruct the result aggregates
	if (aggr.function.destructor) {
		aggr.function.destructor(statef, aggr_input_data, count);
	}
}

unique_ptr<WindowAggregatorState> WindowNaiveAggregator::GetLocalState() const {
	return make_uniq<WindowNaiveState>(*this);
}

void WindowNaiveAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                     const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	const auto &gnstate = gsink.Cast<WindowAggregatorGlobalState>();
	auto &lnstate = lstate.Cast<WindowNaiveState>();
	lnstate.Evaluate(gnstate, bounds, result, count, row_idx);
}

//===--------------------------------------------------------------------===//
// WindowSegmentTree
//===--------------------------------------------------------------------===//
class WindowSegmentTreeGlobalState : public WindowAggregatorGlobalState {
public:
	WindowSegmentTreeGlobalState(const WindowSegmentTree &aggregator, idx_t group_count)
	    : WindowAggregatorGlobalState(aggregator, group_count), tree(aggregator), internal_nodes(0) {
	}
	~WindowSegmentTreeGlobalState() override;

	void ConstructTree();

	//! The owning aggregator
	const WindowSegmentTree &tree;
	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	unsafe_unique_array<data_t> levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;
	//! The total number of internal nodes of the tree, stored in levels_flat_native
	idx_t internal_nodes;

	// TREE_FANOUT needs to cleanly divide STANDARD_VECTOR_SIZE
	static constexpr idx_t TREE_FANOUT = 16;
};

WindowSegmentTree::WindowSegmentTree(AggregateObject aggr, const vector<LogicalType> &arg_types,
                                     const LogicalType &result_type, WindowAggregationMode mode_p,
                                     const WindowExcludeMode exclude_mode_p)
    : WindowAggregator(std::move(aggr), arg_types, result_type, exclude_mode_p), mode(mode_p) {
}

void WindowSegmentTree::Finalize(WindowAggregatorState &gsink, const FrameStats &stats) {
	WindowAggregator::Finalize(gsink, stats);

	auto &gasink = gsink.Cast<WindowSegmentTreeGlobalState>();
	auto &inputs = gasink.inputs;

	if (inputs.ColumnCount() > 0) {
		if (aggr.function.combine && UseCombineAPI()) {
			gasink.ConstructTree();
		}
	}
}

WindowSegmentTreeGlobalState::~WindowSegmentTreeGlobalState() {
	if (!tree.aggr.function.destructor) {
		// nothing to destroy
		return;
	}
	AggregateInputData aggr_input_data(tree.aggr.GetFunctionData(), allocator);
	// call the destructor for all the intermediate states
	data_ptr_t address_data[STANDARD_VECTOR_SIZE];
	Vector addresses(LogicalType::POINTER, data_ptr_cast(address_data));
	idx_t count = 0;
	for (idx_t i = 0; i < internal_nodes; i++) {
		address_data[count++] = data_ptr_t(levels_flat_native.get() + i * tree.state_size);
		if (count == STANDARD_VECTOR_SIZE) {
			tree.aggr.function.destructor(addresses, aggr_input_data, count);
			count = 0;
		}
	}
	if (count > 0) {
		tree.aggr.function.destructor(addresses, aggr_input_data, count);
	}
}

class WindowSegmentTreePart {
public:
	//! Right side nodes need to be cached and processed in reverse order
	using RightEntry = std::pair<idx_t, idx_t>;

	enum FramePart : uint8_t { FULL = 0, LEFT = 1, RIGHT = 2 };

	WindowSegmentTreePart(ArenaAllocator &allocator, const AggregateObject &aggr, const DataChunk &inputs,
	                      const ValidityMask &filter_mask);
	~WindowSegmentTreePart();

	unique_ptr<WindowSegmentTreePart> Copy() const {
		return make_uniq<WindowSegmentTreePart>(allocator, aggr, inputs, filter_mask);
	}

	void FlushStates(bool combining);
	void ExtractFrame(idx_t begin, idx_t end, data_ptr_t current_state);
	void WindowSegmentValue(const WindowSegmentTreeGlobalState &tree, idx_t l_idx, idx_t begin, idx_t end,
	                        data_ptr_t current_state);
	//! Writes result and calls destructors
	void Finalize(Vector &result, idx_t count);

	void Combine(WindowSegmentTreePart &other, idx_t count);

	void Evaluate(const WindowSegmentTreeGlobalState &tree, const idx_t *begins, const idx_t *ends, Vector &result,
	              idx_t count, idx_t row_idx, FramePart frame_part);

protected:
	//! Initialises the accumulation state vector (statef)
	void Initialize(idx_t count);
	//! Accumulate upper tree levels
	void EvaluateUpperLevels(const WindowSegmentTreeGlobalState &tree, const idx_t *begins, const idx_t *ends,
	                         idx_t count, idx_t row_idx, FramePart frame_part);
	void EvaluateLeaves(const WindowSegmentTreeGlobalState &tree, const idx_t *begins, const idx_t *ends, idx_t count,
	                    idx_t row_idx, FramePart frame_part, FramePart leaf_part);

public:
	//! Allocator for aggregates
	ArenaAllocator &allocator;
	//! The aggregate function
	const AggregateObject &aggr;
	//! Order insensitive aggregate (we can optimise internal combines)
	const bool order_insensitive;
	//! The partition arguments
	const DataChunk &inputs;
	//! The filtered rows in inputs
	const ValidityMask &filter_mask;
	//! The size of a single aggregate state
	const idx_t state_size;
	//! Data pointer that contains a vector of states, used for intermediate window segment aggregation
	vector<data_t> state;
	//! Input data chunk, used for leaf segment aggregation
	DataChunk leaves;
	//! The filtered rows in inputs.
	SelectionVector filter_sel;
	//! A vector of pointers to "state", used for intermediate window segment aggregation
	Vector statep;
	//! Reused state pointers for combining segment tree levels
	Vector statel;
	//! Reused result state container for the window functions
	Vector statef;
	//! Count of buffered values
	idx_t flush_count;
	//! Cache of right side tree ranges for ordered aggregates
	vector<RightEntry> right_stack;
};

class WindowSegmentTreeState : public WindowAggregatorState {
public:
	WindowSegmentTreeState() {
	}

	void Evaluate(const WindowSegmentTreeGlobalState &gsink, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx);
	//! The left (default) segment tree part
	unique_ptr<WindowSegmentTreePart> part;
	//! The right segment tree part (for EXCLUDE)
	unique_ptr<WindowSegmentTreePart> right_part;
};

WindowSegmentTreePart::WindowSegmentTreePart(ArenaAllocator &allocator, const AggregateObject &aggr,
                                             const DataChunk &inputs, const ValidityMask &filter_mask)
    : allocator(allocator), aggr(aggr),
      order_insensitive(aggr.function.order_dependent == AggregateOrderDependent::NOT_ORDER_DEPENDENT), inputs(inputs),
      filter_mask(filter_mask), state_size(aggr.function.state_size()), state(state_size * STANDARD_VECTOR_SIZE),
      statep(LogicalType::POINTER), statel(LogicalType::POINTER), statef(LogicalType::POINTER), flush_count(0) {
	if (inputs.ColumnCount() > 0) {
		leaves.Initialize(Allocator::DefaultAllocator(), inputs.GetTypes());
		filter_sel.Initialize();
	}

	//	Build the finalise vector that just points to the result states
	data_ptr_t state_ptr = state.data();
	D_ASSERT(statef.GetVectorType() == VectorType::FLAT_VECTOR);
	statef.SetVectorType(VectorType::CONSTANT_VECTOR);
	statef.Flatten(STANDARD_VECTOR_SIZE);
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		fdata[i] = state_ptr;
		state_ptr += state_size;
	}
}

WindowSegmentTreePart::~WindowSegmentTreePart() {
}

unique_ptr<WindowAggregatorState> WindowSegmentTree::GetGlobalState(idx_t group_count,
                                                                    const ValidityMask &partition_mask) const {
	return make_uniq<WindowSegmentTreeGlobalState>(*this, group_count);
}

unique_ptr<WindowAggregatorState> WindowSegmentTree::GetLocalState() const {
	return make_uniq<WindowSegmentTreeState>();
}

void WindowSegmentTreePart::FlushStates(bool combining) {
	if (!flush_count) {
		return;
	}

	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	if (combining) {
		statel.Verify(flush_count);
		aggr.function.combine(statel, statep, aggr_input_data, flush_count);
	} else {
		leaves.Slice(inputs, filter_sel, flush_count);
		aggr.function.update(&leaves.data[0], aggr_input_data, leaves.ColumnCount(), statep, flush_count);
	}

	flush_count = 0;
}

void WindowSegmentTreePart::Combine(WindowSegmentTreePart &other, idx_t count) {
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.combine(other.statef, statef, aggr_input_data, count);
}

void WindowSegmentTreePart::ExtractFrame(idx_t begin, idx_t end, data_ptr_t state_ptr) {
	const auto count = end - begin;

	//	If we are not filtering,
	//	just update the shared dictionary selection to the range
	//	Otherwise set it to the input rows that pass the filter
	auto states = FlatVector::GetData<data_ptr_t>(statep);
	if (filter_mask.AllValid()) {
		for (idx_t i = 0; i < count; ++i) {
			states[flush_count] = state_ptr;
			filter_sel.set_index(flush_count++, begin + i);
			if (flush_count >= STANDARD_VECTOR_SIZE) {
				FlushStates(false);
			}
		}
	} else {
		for (idx_t i = begin; i < end; ++i) {
			if (filter_mask.RowIsValid(i)) {
				states[flush_count] = state_ptr;
				filter_sel.set_index(flush_count++, i);
				if (flush_count >= STANDARD_VECTOR_SIZE) {
					FlushStates(false);
				}
			}
		}
	}
}

void WindowSegmentTreePart::WindowSegmentValue(const WindowSegmentTreeGlobalState &tree, idx_t l_idx, idx_t begin,
                                               idx_t end, data_ptr_t state_ptr) {
	D_ASSERT(begin <= end);
	if (begin == end || inputs.ColumnCount() == 0) {
		return;
	}

	const auto count = end - begin;
	if (l_idx == 0) {
		ExtractFrame(begin, end, state_ptr);
	} else {
		// find out where the states begin
		auto begin_ptr = tree.levels_flat_native.get() + state_size * (begin + tree.levels_flat_start[l_idx - 1]);
		// set up a vector of pointers that point towards the set of states
		auto ldata = FlatVector::GetData<data_ptr_t>(statel);
		auto pdata = FlatVector::GetData<data_ptr_t>(statep);
		for (idx_t i = 0; i < count; i++) {
			pdata[flush_count] = state_ptr;
			ldata[flush_count++] = begin_ptr;
			begin_ptr += state_size;
			if (flush_count >= STANDARD_VECTOR_SIZE) {
				FlushStates(true);
			}
		}
	}
}
void WindowSegmentTreePart::Finalize(Vector &result, idx_t count) {
	//	Finalise the result aggregates and write to result if write_result is set
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.finalize(statef, aggr_input_data, result, count, 0);

	//	Destruct the result aggregates
	if (aggr.function.destructor) {
		aggr.function.destructor(statef, aggr_input_data, count);
	}
}

void WindowSegmentTreeGlobalState::ConstructTree() {
	D_ASSERT(inputs.ColumnCount() > 0);

	//	Single part for constructing the tree
	WindowSegmentTreePart gtstate(allocator, tree.aggr, inputs, filter_mask);

	// compute space required to store internal nodes of segment tree
	internal_nodes = 0;
	idx_t level_nodes = inputs.size();
	do {
		level_nodes = (level_nodes + (TREE_FANOUT - 1)) / TREE_FANOUT;
		internal_nodes += level_nodes;
	} while (level_nodes > 1);
	levels_flat_native = make_unsafe_uniq_array<data_t>(internal_nodes * tree.state_size);
	levels_flat_start.push_back(0);

	idx_t levels_flat_offset = 0;
	idx_t level_current = 0;
	// level 0 is data itself
	idx_t level_size;
	// iterate over the levels of the segment tree
	while ((level_size =
	            (level_current == 0 ? inputs.size() : levels_flat_offset - levels_flat_start[level_current - 1])) > 1) {
		for (idx_t pos = 0; pos < level_size; pos += TREE_FANOUT) {
			// compute the aggregate for this entry in the segment tree
			data_ptr_t state_ptr = levels_flat_native.get() + (levels_flat_offset * tree.state_size);
			gtstate.aggr.function.initialize(state_ptr);
			gtstate.WindowSegmentValue(*this, level_current, pos, MinValue(level_size, pos + TREE_FANOUT), state_ptr);
			gtstate.FlushStates(level_current > 0);

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}

	// Corner case: single element in the window
	if (levels_flat_offset == 0) {
		gtstate.aggr.function.initialize(levels_flat_native.get());
	}
}

void WindowSegmentTree::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                 const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	const auto &gtstate = gsink.Cast<WindowSegmentTreeGlobalState>();
	auto &ltstate = lstate.Cast<WindowSegmentTreeState>();
	ltstate.Evaluate(gtstate, bounds, result, count, row_idx);
}

void WindowSegmentTreeState::Evaluate(const WindowSegmentTreeGlobalState &gtstate, const DataChunk &bounds,
                                      Vector &result, idx_t count, idx_t row_idx) {
	auto window_begin = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_BEGIN]);
	auto window_end = FlatVector::GetData<const idx_t>(bounds.data[WINDOW_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(bounds.data[PEER_BEGIN]);
	auto peer_end = FlatVector::GetData<const idx_t>(bounds.data[PEER_END]);

	if (!part) {
		part =
		    make_uniq<WindowSegmentTreePart>(allocator, gtstate.aggregator.aggr, gtstate.inputs, gtstate.filter_mask);
	}

	if (gtstate.aggregator.exclude_mode != WindowExcludeMode::NO_OTHER) {
		// 1. evaluate the tree left of the excluded part
		part->Evaluate(gtstate, window_begin, peer_begin, result, count, row_idx, WindowSegmentTreePart::LEFT);

		// 2. set up a second state for the right of the excluded part
		if (!right_part) {
			right_part = part->Copy();
		}

		// 3. evaluate the tree right of the excluded part
		right_part->Evaluate(gtstate, peer_end, window_end, result, count, row_idx, WindowSegmentTreePart::RIGHT);

		// 4. combine the buffer state into the Segment Tree State
		part->Combine(*right_part, count);
	} else {
		part->Evaluate(gtstate, window_begin, window_end, result, count, row_idx, WindowSegmentTreePart::FULL);
	}

	part->Finalize(result, count);
}

void WindowSegmentTreePart::Evaluate(const WindowSegmentTreeGlobalState &tree, const idx_t *begins, const idx_t *ends,
                                     Vector &result, idx_t count, idx_t row_idx, FramePart frame_part) {
	Initialize(count);

	if (order_insensitive) {
		//	First pass: aggregate the segment tree nodes with sharing
		EvaluateUpperLevels(tree, begins, ends, count, row_idx, frame_part);

		//	Second pass: aggregate the ragged leaves
		EvaluateLeaves(tree, begins, ends, count, row_idx, frame_part, FramePart::FULL);
	} else {
		//	Evaluate leaves in order
		EvaluateLeaves(tree, begins, ends, count, row_idx, frame_part, FramePart::LEFT);
		EvaluateUpperLevels(tree, begins, ends, count, row_idx, frame_part);
		EvaluateLeaves(tree, begins, ends, count, row_idx, frame_part, FramePart::RIGHT);
	}
}

void WindowSegmentTreePart::Initialize(idx_t count) {
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	for (idx_t rid = 0; rid < count; ++rid) {
		auto state_ptr = fdata[rid];
		aggr.function.initialize(state_ptr);
	}
}

void WindowSegmentTreePart::EvaluateUpperLevels(const WindowSegmentTreeGlobalState &tree, const idx_t *begins,
                                                const idx_t *ends, idx_t count, idx_t row_idx, FramePart frame_part) {
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);

	const auto exclude_mode = tree.tree.exclude_mode;
	const bool begin_on_curr_row = frame_part == FramePart::RIGHT && exclude_mode == WindowExcludeMode::CURRENT_ROW;
	const bool end_on_curr_row = frame_part == FramePart::LEFT && exclude_mode == WindowExcludeMode::CURRENT_ROW;

	const auto max_level = tree.levels_flat_start.size() + 1;
	right_stack.resize(max_level, {0, 0});

	//	Share adjacent identical states
	//  We do this first because we want to share only tree aggregations
	idx_t prev_begin = 1;
	idx_t prev_end = 0;
	auto ldata = FlatVector::GetData<data_ptr_t>(statel);
	auto pdata = FlatVector::GetData<data_ptr_t>(statep);
	data_ptr_t prev_state = nullptr;
	for (idx_t rid = 0, cur_row = row_idx; rid < count; ++rid, ++cur_row) {
		auto state_ptr = fdata[rid];

		auto begin = begin_on_curr_row ? cur_row + 1 : begins[rid];
		auto end = end_on_curr_row ? cur_row : ends[rid];
		if (begin >= end) {
			continue;
		}

		//	Skip level 0
		idx_t l_idx = 0;
		idx_t right_max = 0;
		for (; l_idx < max_level; l_idx++) {
			idx_t parent_begin = begin / tree.TREE_FANOUT;
			idx_t parent_end = end / tree.TREE_FANOUT;
			if (prev_state && l_idx == 1 && begin == prev_begin && end == prev_end) {
				//	Just combine the previous top level result
				ldata[flush_count] = prev_state;
				pdata[flush_count] = state_ptr;
				if (++flush_count >= STANDARD_VECTOR_SIZE) {
					FlushStates(true);
				}
				break;
			}

			if (order_insensitive && l_idx == 1) {
				prev_state = state_ptr;
				prev_begin = begin;
				prev_end = end;
			}

			if (parent_begin == parent_end) {
				if (l_idx) {
					WindowSegmentValue(tree, l_idx, begin, end, state_ptr);
				}
				break;
			}
			idx_t group_begin = parent_begin * tree.TREE_FANOUT;
			if (begin != group_begin) {
				if (l_idx) {
					WindowSegmentValue(tree, l_idx, begin, group_begin + tree.TREE_FANOUT, state_ptr);
				}
				parent_begin++;
			}
			idx_t group_end = parent_end * tree.TREE_FANOUT;
			if (end != group_end) {
				if (l_idx) {
					if (order_insensitive) {
						WindowSegmentValue(tree, l_idx, group_end, end, state_ptr);
					} else {
						right_stack[l_idx] = {group_end, end};
						right_max = l_idx;
					}
				}
			}
			begin = parent_begin;
			end = parent_end;
		}

		// Flush the right side values from left to right for order_sensitive aggregates
		// As we go up the tree, the right side ranges move left,
		// so we just cache them in a fixed size, preallocated array.
		// Then we can just reverse scan the array and append the cached ranges.
		for (l_idx = right_max; l_idx > 0; --l_idx) {
			auto &right_entry = right_stack[l_idx];
			const auto group_end = right_entry.first;
			const auto end = right_entry.second;
			if (end) {
				WindowSegmentValue(tree, l_idx, group_end, end, state_ptr);
				right_entry = {0, 0};
			}
		}
	}
	FlushStates(true);
}

void WindowSegmentTreePart::EvaluateLeaves(const WindowSegmentTreeGlobalState &tree, const idx_t *begins,
                                           const idx_t *ends, idx_t count, idx_t row_idx, FramePart frame_part,
                                           FramePart leaf_part) {

	auto fdata = FlatVector::GetData<data_ptr_t>(statef);

	// For order-sensitive aggregates, we have to process the ragged leaves in two pieces.
	// The left side have to be added before the main tree followed by the ragged right sides.
	// The current row is the leftmost value of the right hand side.
	const bool compute_left = leaf_part != FramePart::RIGHT;
	const bool compute_right = leaf_part != FramePart::LEFT;
	const auto exclude_mode = tree.tree.exclude_mode;
	const bool begin_on_curr_row = frame_part == FramePart::RIGHT && exclude_mode == WindowExcludeMode::CURRENT_ROW;
	const bool end_on_curr_row = frame_part == FramePart::LEFT && exclude_mode == WindowExcludeMode::CURRENT_ROW;
	// with EXCLUDE TIES, in addition to the frame part right of the peer group's end, we also need to consider the
	// current row
	const bool add_curr_row = compute_left && frame_part == FramePart::RIGHT && exclude_mode == WindowExcludeMode::TIES;

	for (idx_t rid = 0, cur_row = row_idx; rid < count; ++rid, ++cur_row) {
		auto state_ptr = fdata[rid];

		const auto begin = begin_on_curr_row ? cur_row + 1 : begins[rid];
		const auto end = end_on_curr_row ? cur_row : ends[rid];
		if (add_curr_row) {
			WindowSegmentValue(tree, 0, cur_row, cur_row + 1, state_ptr);
		}
		if (begin >= end) {
			continue;
		}

		idx_t parent_begin = begin / tree.TREE_FANOUT;
		idx_t parent_end = end / tree.TREE_FANOUT;
		if (parent_begin == parent_end) {
			if (compute_left) {
				WindowSegmentValue(tree, 0, begin, end, state_ptr);
			}
			continue;
		}

		idx_t group_begin = parent_begin * tree.TREE_FANOUT;
		if (begin != group_begin && compute_left) {
			WindowSegmentValue(tree, 0, begin, group_begin + tree.TREE_FANOUT, state_ptr);
		}
		idx_t group_end = parent_end * tree.TREE_FANOUT;
		if (end != group_end && compute_right) {
			WindowSegmentValue(tree, 0, group_end, end, state_ptr);
		}
	}
	FlushStates(false);
}

//===--------------------------------------------------------------------===//
// WindowDistinctAggregator
//===--------------------------------------------------------------------===//
WindowDistinctAggregator::WindowDistinctAggregator(AggregateObject aggr, const vector<LogicalType> &arg_types,
                                                   const LogicalType &result_type,
                                                   const WindowExcludeMode exclude_mode_p, ClientContext &context)
    : WindowAggregator(std::move(aggr), arg_types, result_type, exclude_mode_p), context(context) {
}

class WindowDistinctAggregatorGlobalState : public WindowAggregatorGlobalState {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	class DistinctSortTree;

	WindowDistinctAggregatorGlobalState(const WindowDistinctAggregator &aggregator, idx_t group_count);
	~WindowDistinctAggregatorGlobalState() override;

	void Sink(DataChunk &arg_chunk, SelectionVector *filter_sel, idx_t filtered);
	void Finalize(const FrameStats &stats);

	//	Single threaded sorting for now
	ClientContext &context;
	GlobalSortStatePtr global_sort;
	LocalSortState local_sort;
	idx_t payload_pos;
	idx_t memory_per_thread;

	vector<LogicalType> payload_types;
	DataChunk sort_chunk;
	DataChunk payload_chunk;

	//! The merge sort tree for the aggregate.
	unique_ptr<DistinctSortTree> merge_sort_tree;

	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	unsafe_unique_array<data_t> levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;

	//! The total number of internal nodes of the tree, stored in levels_flat_native
	idx_t internal_nodes;
};

WindowDistinctAggregatorGlobalState::WindowDistinctAggregatorGlobalState(const WindowDistinctAggregator &aggregator_p,
                                                                         idx_t group_count)
    : WindowAggregatorGlobalState(aggregator_p, group_count), context(aggregator_p.context) {
	payload_types.emplace_back(LogicalType::UBIGINT);
	payload_chunk.Initialize(Allocator::DefaultAllocator(), payload_types);
}

WindowDistinctAggregatorGlobalState::~WindowDistinctAggregatorGlobalState() {
	const auto &aggr = aggregator.aggr;
	if (!aggr.function.destructor) {
		// nothing to destroy
		return;
	}
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	// call the destructor for all the intermediate states
	data_ptr_t address_data[STANDARD_VECTOR_SIZE];
	Vector addresses(LogicalType::POINTER, data_ptr_cast(address_data));
	idx_t count = 0;
	for (idx_t i = 0; i < internal_nodes; i++) {
		address_data[count++] = data_ptr_t(levels_flat_native.get() + i * aggregator.state_size);
		if (count == STANDARD_VECTOR_SIZE) {
			aggr.function.destructor(addresses, aggr_input_data, count);
			count = 0;
		}
	}
	if (count > 0) {
		aggr.function.destructor(addresses, aggr_input_data, count);
	}
}

unique_ptr<WindowAggregatorState> WindowDistinctAggregator::GetGlobalState(idx_t group_count,
                                                                           const ValidityMask &partition_mask) const {
	return make_uniq<WindowDistinctAggregatorGlobalState>(*this, group_count);
}

void WindowDistinctAggregator::Sink(WindowAggregatorState &gsink, DataChunk &arg_chunk, SelectionVector *filter_sel,
                                    idx_t filtered) {
	WindowAggregator::Sink(gsink, arg_chunk, filter_sel, filtered);

	auto &gdstate = gsink.Cast<WindowDistinctAggregatorGlobalState>();
	gdstate.Sink(arg_chunk, filter_sel, filtered);
}

void WindowDistinctAggregatorGlobalState::Sink(DataChunk &arg_chunk, SelectionVector *filter_sel, idx_t filtered) {
	//	We sort the arguments and use the partition index as a tie-breaker.
	//	TODO: Use a hash table?
	if (!global_sort) {
		//	1:	functionComputePrevIdcs(𝑖𝑛)
		//	2:		sorted ← []
		vector<LogicalType> sort_types;
		for (const auto &col : arg_chunk.data) {
			sort_types.emplace_back(col.GetType());
		}

		for (const auto &type : payload_types) {
			sort_types.emplace_back(type);
		}

		vector<BoundOrderByNode> orders;
		for (const auto &type : sort_types) {
			auto expr = make_uniq<BoundConstantExpression>(Value(type));
			orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, std::move(expr)));
		}

		RowLayout payload_layout;
		payload_layout.Initialize(payload_types);

		global_sort = make_uniq<GlobalSortState>(BufferManager::GetBufferManager(context), orders, payload_layout);
		local_sort.Initialize(*global_sort, global_sort->buffer_manager);

		sort_chunk.Initialize(Allocator::DefaultAllocator(), sort_types);
		sort_chunk.data.back().Reference(payload_chunk.data[0]);
		payload_pos = 0;
		memory_per_thread = PhysicalOperator::GetMaxThreadMemory(context);
	}

	//	3: 	for i ← 0 to in.size do
	//	4: 		sorted[i] ← (in[i], i)
	const auto count = arg_chunk.size();
	payload_chunk.Reset();
	auto &sorted_vec = payload_chunk.data[0];
	auto sorted = FlatVector::GetData<idx_t>(sorted_vec);
	std::iota(sorted, sorted + count, payload_pos);
	payload_pos += count;

	for (column_t c = 0; c < arg_chunk.ColumnCount(); ++c) {
		sort_chunk.data[c].Reference(arg_chunk.data[c]);
	}
	sort_chunk.data.back().Reference(sorted_vec);
	sort_chunk.SetCardinality(arg_chunk);
	payload_chunk.SetCardinality(sort_chunk);

	//	Apply FILTER clause, if any
	if (filter_sel) {
		sort_chunk.Slice(*filter_sel, filtered);
		payload_chunk.Slice(*filter_sel, filtered);
	}

	local_sort.SinkChunk(sort_chunk, payload_chunk);

	if (local_sort.SizeInBytes() > memory_per_thread) {
		local_sort.Sort(*global_sort, true);
	}
}

void WindowDistinctAggregator::Finalize(WindowAggregatorState &gsink, const FrameStats &stats) {
	auto &gdsink = gsink.Cast<WindowDistinctAggregatorGlobalState>();
	gdsink.Finalize(stats);
}

class WindowDistinctAggregatorGlobalState::DistinctSortTree : public MergeSortTree<idx_t, idx_t> {
public:
	// prev_idx, input_idx
	using ZippedTuple = std::tuple<idx_t, idx_t>;
	using ZippedElements = vector<ZippedTuple>;

	DistinctSortTree(ZippedElements &&prev_idcs, WindowDistinctAggregatorGlobalState &gdsink);
};

void WindowDistinctAggregatorGlobalState::Finalize(const FrameStats &stats) {
	//	5: Sort sorted lexicographically increasing
	global_sort->AddLocalState(local_sort);
	global_sort->PrepareMergePhase();
	while (global_sort->sorted_blocks.size() > 1) {
		global_sort->InitializeMergeRound();
		MergeSorter merge_sorter(*global_sort, global_sort->buffer_manager);
		merge_sorter.PerformInMergeRound();
		global_sort->CompleteMergeRound(true);
	}

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::DefaultAllocator(), payload_types);

	auto scanner = make_uniq<PayloadScanner>(*global_sort);
	const auto in_size = scanner->Remaining();
	scanner->Scan(scan_chunk);
	idx_t scan_idx = 0;

	//	6:	prevIdcs ← []
	//	7:	prevIdcs[0] ← “-”
	const auto count = inputs.size();
	using ZippedTuple = DistinctSortTree::ZippedTuple;
	DistinctSortTree::ZippedElements prev_idcs;
	prev_idcs.resize(count);

	//	To handle FILTER clauses we make the missing elements
	//	point to themselves so they won't be counted.
	if (in_size < count) {
		for (idx_t i = 0; i < count; ++i) {
			prev_idcs[i] = ZippedTuple(i + 1, i);
		}
	}

	auto *input_idx = FlatVector::GetData<idx_t>(scan_chunk.data[0]);
	auto i = input_idx[scan_idx++];
	prev_idcs[i] = ZippedTuple(0, i);

	SBIterator curr(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator prev(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	auto prefix_layout = global_sort->sort_layout.GetPrefixComparisonLayout(sort_chunk.ColumnCount() - 1);

	//	8:	for i ← 1 to in.size do
	for (++curr; curr.GetIndex() < in_size; ++curr, ++prev) {
		//	Scan second one chunk at a time
		//	Note the scan is one behind the iterators
		if (scan_idx >= scan_chunk.size()) {
			scan_chunk.Reset();
			scanner->Scan(scan_chunk);
			scan_idx = 0;
			input_idx = FlatVector::GetData<idx_t>(scan_chunk.data[0]);
		}
		auto second = i;
		i = input_idx[scan_idx++];

		int lt = 0;
		if (prefix_layout.all_constant) {
			lt = FastMemcmp(prev.entry_ptr, curr.entry_ptr, prefix_layout.comparison_size);
		} else {
			lt = Comparators::CompareTuple(prev.scan, curr.scan, prev.entry_ptr, curr.entry_ptr, prefix_layout,
			                               prev.external);
		}

		//	9:	if sorted[i].first == sorted[i-1].first then
		//	10:		prevIdcs[i] ← sorted[i-1].second
		//	11:	else
		//	12:		prevIdcs[i] ← “-”
		if (!lt) {
			prev_idcs[i] = ZippedTuple(second + 1, i);
		} else {
			prev_idcs[i] = ZippedTuple(0, i);
		}
	}
	//	13:	return prevIdcs

	merge_sort_tree = make_uniq<DistinctSortTree>(std::move(prev_idcs), *this);
}

WindowDistinctAggregatorGlobalState::DistinctSortTree::DistinctSortTree(ZippedElements &&prev_idcs,
                                                                        WindowDistinctAggregatorGlobalState &gdsink) {
	auto &aggr = gdsink.aggregator.aggr;
	auto &allocator = gdsink.allocator;
	auto &inputs = gdsink.inputs;
	const auto state_size = gdsink.aggregator.state_size;
	auto &internal_nodes = gdsink.internal_nodes;
	auto &levels_flat_native = gdsink.levels_flat_native;
	auto &levels_flat_start = gdsink.levels_flat_start;

	//! Input data chunk, used for leaf segment aggregation
	DataChunk leaves;
	leaves.Initialize(Allocator::DefaultAllocator(), gdsink.inputs.GetTypes());
	SelectionVector sel;
	sel.Initialize();

	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);

	//! The states to update
	Vector update_v(LogicalType::POINTER);
	auto updates = FlatVector::GetData<data_ptr_t>(update_v);
	idx_t nupdate = 0;

	Vector source_v(LogicalType::POINTER);
	auto sources = FlatVector::GetData<data_ptr_t>(source_v);
	Vector target_v(LogicalType::POINTER);
	auto targets = FlatVector::GetData<data_ptr_t>(target_v);
	idx_t ncombine = 0;

	// compute space required to store aggregation states of merge sort tree
	// this is one aggregate state per entry per level
	MergeSortTree<ZippedTuple> zipped_tree(std::move(prev_idcs));
	internal_nodes = 0;
	for (idx_t level_nr = 0; level_nr < zipped_tree.tree.size(); ++level_nr) {
		internal_nodes += zipped_tree.tree[level_nr].first.size();
	}
	levels_flat_native = make_unsafe_uniq_array<data_t>(internal_nodes * state_size);
	levels_flat_start.push_back(0);
	idx_t levels_flat_offset = 0;

	//	Walk the distinct value tree building the intermediate aggregates
	tree.reserve(zipped_tree.tree.size());
	idx_t level_width = 1;
	for (idx_t level_nr = 0; level_nr < zipped_tree.tree.size(); ++level_nr) {
		auto &zipped_level = zipped_tree.tree[level_nr].first;
		vector<ElementType> level;
		level.reserve(zipped_level.size());

		for (idx_t i = 0; i < zipped_level.size(); i += level_width) {
			//	Reset the combine state
			data_ptr_t prev_state = nullptr;
			auto next_limit = MinValue<idx_t>(zipped_level.size(), i + level_width);
			for (auto j = i; j < next_limit; ++j) {
				//	Initialise the next aggregate
				auto curr_state = levels_flat_native.get() + (levels_flat_offset++ * state_size);
				aggr.function.initialize(curr_state);

				//	Update this state (if it matches)
				const auto prev_idx = std::get<0>(zipped_level[j]);
				level.emplace_back(prev_idx);
				if (prev_idx < i + 1) {
					updates[nupdate] = curr_state;
					//	input_idx
					sel[nupdate] = UnsafeNumericCast<sel_t>(std::get<1>(zipped_level[j]));
					++nupdate;
				}

				//	Merge the previous state (if any)
				if (prev_state) {
					sources[ncombine] = prev_state;
					targets[ncombine] = curr_state;
					++ncombine;
				}
				prev_state = curr_state;

				//	Flush the states if one is maxed out.
				if (MaxValue<idx_t>(ncombine, nupdate) >= STANDARD_VECTOR_SIZE) {
					//	Push the updates first so they propagate
					leaves.Reference(inputs);
					leaves.Slice(sel, nupdate);
					aggr.function.update(leaves.data.data(), aggr_input_data, leaves.ColumnCount(), update_v, nupdate);
					nupdate = 0;

					//	Combine the states sequentially
					aggr.function.combine(source_v, target_v, aggr_input_data, ncombine);
					ncombine = 0;
				}
			}
		}

		tree.emplace_back(std::move(level), std::move(zipped_tree.tree[level_nr].second));

		levels_flat_start.push_back(levels_flat_offset);
		level_width *= FANOUT;
	}

	//	Flush any remaining states
	if (ncombine || nupdate) {
		//	Push  the updates
		leaves.Reference(inputs);
		leaves.Slice(sel, nupdate);
		aggr.function.update(leaves.data.data(), aggr_input_data, leaves.ColumnCount(), update_v, nupdate);
		nupdate = 0;

		//	Combine the states sequentially
		aggr.function.combine(source_v, target_v, aggr_input_data, ncombine);
		ncombine = 0;
	}
}

class WindowDistinctState : public WindowAggregatorState {
public:
	explicit WindowDistinctState(const WindowDistinctAggregator &aggregator);

	void Evaluate(const WindowDistinctAggregatorGlobalState &gdstate, const DataChunk &bounds, Vector &result,
	              idx_t count, idx_t row_idx);

protected:
	//! Flush the accumulated intermediate states into the result states
	void FlushStates();

	//! The aggregator we are working with
	const WindowDistinctAggregator &aggregator;
	//! The size of a single aggregate state
	const idx_t state_size;
	//! Data pointer that contains a vector of states, used for row aggregation
	vector<data_t> state;
	//! Reused result state container for the window functions
	Vector statef;
	//! A vector of pointers to "state", used for buffering intermediate aggregates
	Vector statep;
	//! Reused state pointers for combining tree elements
	Vector statel;
	//! Count of buffered values
	idx_t flush_count;
	//! The frame boundaries, used for the window functions
	SubFrames frames;
};

WindowDistinctState::WindowDistinctState(const WindowDistinctAggregator &aggregator)
    : aggregator(aggregator), state_size(aggregator.state_size), state((aggregator.state_size * STANDARD_VECTOR_SIZE)),
      statef(LogicalType::POINTER), statep(LogicalType::POINTER), statel(LogicalType::POINTER), flush_count(0) {
	InitSubFrames(frames, aggregator.exclude_mode);

	//	Build the finalise vector that just points to the result states
	data_ptr_t state_ptr = state.data();
	D_ASSERT(statef.GetVectorType() == VectorType::FLAT_VECTOR);
	statef.SetVectorType(VectorType::CONSTANT_VECTOR);
	statef.Flatten(STANDARD_VECTOR_SIZE);
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		fdata[i] = state_ptr;
		state_ptr += state_size;
	}
}

void WindowDistinctState::FlushStates() {
	if (!flush_count) {
		return;
	}

	const auto &aggr = aggregator.aggr;
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	statel.Verify(flush_count);
	aggr.function.combine(statel, statep, aggr_input_data, flush_count);

	flush_count = 0;
}

void WindowDistinctState::Evaluate(const WindowDistinctAggregatorGlobalState &gdstate, const DataChunk &bounds,
                                   Vector &result, idx_t count, idx_t row_idx) {
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	auto ldata = FlatVector::GetData<data_ptr_t>(statel);
	auto pdata = FlatVector::GetData<data_ptr_t>(statep);

	const auto &merge_sort_tree = *gdstate.merge_sort_tree;
	const auto running_aggs = gdstate.levels_flat_native.get();
	const auto exclude_mode = gdstate.aggregator.exclude_mode;
	const auto &aggr = gdstate.aggregator.aggr;

	EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t rid) {
		auto agg_state = fdata[rid];
		aggr.function.initialize(agg_state);

		//	TODO: Extend AggregateLowerBound to handle subframes, just like SelectNth.
		const auto lower = frames[0].start;
		const auto upper = frames[0].end;
		merge_sort_tree.AggregateLowerBound(lower, upper, lower + 1,
		                                    [&](idx_t level, const idx_t run_begin, const idx_t run_pos) {
			                                    if (run_pos != run_begin) {
				                                    //	Find the source aggregate
				                                    // Buffer a merge of the indicated state into the current state
				                                    const auto agg_idx = gdstate.levels_flat_start[level] + run_pos - 1;
				                                    const auto running_agg = running_aggs + agg_idx * state_size;
				                                    pdata[flush_count] = agg_state;
				                                    ldata[flush_count++] = running_agg;
				                                    if (flush_count >= STANDARD_VECTOR_SIZE) {
					                                    FlushStates();
				                                    }
			                                    }
		                                    });
	});

	//	Flush the final states
	FlushStates();

	//	Finalise the result aggregates and write to the result
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.finalize(statef, aggr_input_data, result, count, 0);

	//	Destruct the result aggregates
	if (aggr.function.destructor) {
		aggr.function.destructor(statef, aggr_input_data, count);
	}
}

unique_ptr<WindowAggregatorState> WindowDistinctAggregator::GetLocalState() const {
	return make_uniq<WindowDistinctState>(*this);
}

void WindowDistinctAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                        const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {

	const auto &gdstate = gsink.Cast<WindowDistinctAggregatorGlobalState>();
	auto &ldstate = lstate.Cast<WindowDistinctState>();
	ldstate.Evaluate(gdstate, bounds, result, count, row_idx);
}

} // namespace duckdb
