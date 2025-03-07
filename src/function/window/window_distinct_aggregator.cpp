#include "duckdb/function/window/window_distinct_aggregator.hpp"

#include "duckdb/common/sort/partition_state.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/function/window/window_aggregate_states.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <numeric>
#include <thread>

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowDistinctAggregator
//===--------------------------------------------------------------------===//
bool WindowDistinctAggregator::CanAggregate(const BoundWindowExpression &wexpr) {
	if (!wexpr.aggregate) {
		return false;
	}

	return wexpr.distinct && wexpr.exclude_clause == WindowExcludeMode::NO_OTHER && wexpr.arg_orders.empty();
}

WindowDistinctAggregator::WindowDistinctAggregator(const BoundWindowExpression &wexpr, WindowSharedExpressions &shared,
                                                   ClientContext &context)
    : WindowAggregator(wexpr, shared), context(context) {
}

class WindowDistinctAggregatorLocalState;

class WindowDistinctAggregatorGlobalState;

class WindowDistinctSortTree : public MergeSortTree<idx_t, idx_t> {
public:
	// prev_idx, input_idx
	using ZippedTuple = std::tuple<idx_t, idx_t>;
	using ZippedElements = vector<ZippedTuple>;

	explicit WindowDistinctSortTree(WindowDistinctAggregatorGlobalState &gdastate, idx_t count) : gdastate(gdastate) {
		//	Set up for parallel build
		build_level = 0;
		build_complete = 0;
		build_run = 0;
		build_run_length = 1;
		build_num_runs = count;
	}

	void Build(WindowDistinctAggregatorLocalState &ldastate);

protected:
	bool TryNextRun(idx_t &level_idx, idx_t &run_idx);
	void BuildRun(idx_t level_nr, idx_t i, WindowDistinctAggregatorLocalState &ldastate);

	WindowDistinctAggregatorGlobalState &gdastate;
};

class WindowDistinctAggregatorGlobalState : public WindowAggregatorGlobalState {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	using LocalSortStatePtr = unique_ptr<LocalSortState>;
	using ZippedTuple = WindowDistinctSortTree::ZippedTuple;
	using ZippedElements = WindowDistinctSortTree::ZippedElements;

	WindowDistinctAggregatorGlobalState(ClientContext &context, const WindowDistinctAggregator &aggregator,
	                                    idx_t group_count);

	//! Compute the block starts
	void MeasurePayloadBlocks();
	//! Create a new local sort
	optional_ptr<LocalSortState> InitializeLocalSort() const;

	//! Patch up the previous index block boundaries
	void PatchPrevIdcs();
	bool TryPrepareNextStage(WindowDistinctAggregatorLocalState &lstate);

	//	Single threaded sorting for now
	ClientContext &context;
	idx_t memory_per_thread;

	//! Finalize guard
	mutable mutex lock;
	//! Finalize stage
	atomic<PartitionSortStage> stage;
	//! Tasks launched
	idx_t total_tasks = 0;
	//! Tasks launched
	mutable idx_t tasks_assigned;
	//! Tasks landed
	mutable atomic<idx_t> tasks_completed;

	//! The sorted payload data types (partition index)
	vector<LogicalType> payload_types;
	//! The aggregate arguments + partition index
	vector<LogicalType> sort_types;

	//! Sorting operations
	GlobalSortStatePtr global_sort;
	//! Local sort set
	mutable vector<LocalSortStatePtr> local_sorts;
	//! The block starts (the scanner doesn't know this) plus the total count
	vector<idx_t> block_starts;

	//! The block boundary seconds
	mutable ZippedElements seconds;
	//! The MST with the distinct back pointers
	mutable MergeSortTree<ZippedTuple> zipped_tree;
	//! The merge sort tree for the aggregate.
	WindowDistinctSortTree merge_sort_tree;

	//! The actual window segment tree: an array of aggregate states that represent all the intermediate nodes
	WindowAggregateStates levels_flat_native;
	//! For each level, the starting location in the levels_flat_native array
	vector<idx_t> levels_flat_start;
};

WindowDistinctAggregatorGlobalState::WindowDistinctAggregatorGlobalState(ClientContext &context,
                                                                         const WindowDistinctAggregator &aggregator,
                                                                         idx_t group_count)
    : WindowAggregatorGlobalState(context, aggregator, group_count), context(aggregator.context),
      stage(PartitionSortStage::INIT), tasks_assigned(0), tasks_completed(0), merge_sort_tree(*this, group_count),
      levels_flat_native(aggr) {
	payload_types.emplace_back(LogicalType::UBIGINT);

	//	1:	functionComputePrevIdcs(𝑖𝑛)
	//	2:		sorted ← []
	//	We sort the aggregate arguments and use the partition index as a tie-breaker.
	//	TODO: Use a hash table?
	sort_types = aggregator.arg_types;
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

	global_sort = make_uniq<GlobalSortState>(context, orders, payload_layout);

	memory_per_thread = PhysicalOperator::GetMaxThreadMemory(context);

	//	6:	prevIdcs ← []
	//	7:	prevIdcs[0] ← “-”
	auto &prev_idcs = zipped_tree.Allocate(group_count);

	//	To handle FILTER clauses we make the missing elements
	//	point to themselves so they won't be counted.
	for (idx_t i = 0; i < group_count; ++i) {
		prev_idcs[i] = ZippedTuple(i + 1, i);
	}

	// compute space required to store aggregation states of merge sort tree
	// this is one aggregate state per entry per level
	idx_t internal_nodes = 0;
	levels_flat_start.push_back(internal_nodes);
	for (idx_t level_nr = 0; level_nr < zipped_tree.tree.size(); ++level_nr) {
		internal_nodes += zipped_tree.tree[level_nr].first.size();
		levels_flat_start.push_back(internal_nodes);
	}
	levels_flat_native.Initialize(internal_nodes);

	merge_sort_tree.tree.reserve(zipped_tree.tree.size());
	for (idx_t level_nr = 0; level_nr < zipped_tree.tree.size(); ++level_nr) {
		auto &zipped_level = zipped_tree.tree[level_nr].first;
		WindowDistinctSortTree::Elements level;
		WindowDistinctSortTree::Offsets cascades;
		level.resize(zipped_level.size());
		merge_sort_tree.tree.emplace_back(std::move(level), std::move(cascades));
	}
}

optional_ptr<LocalSortState> WindowDistinctAggregatorGlobalState::InitializeLocalSort() const {
	lock_guard<mutex> local_sort_guard(lock);
	auto local_sort = make_uniq<LocalSortState>();
	local_sort->Initialize(*global_sort, global_sort->buffer_manager);
	++tasks_assigned;
	local_sorts.emplace_back(std::move(local_sort));

	return local_sorts.back().get();
}

class WindowDistinctAggregatorLocalState : public WindowAggregatorLocalState {
public:
	explicit WindowDistinctAggregatorLocalState(const WindowDistinctAggregatorGlobalState &aggregator);

	~WindowDistinctAggregatorLocalState() override {
		statef.Destroy();
	}

	void Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel,
	          idx_t filtered);
	void Finalize(WindowAggregatorGlobalState &gastate, CollectionPtr collection) override;
	void Sorted();
	void ExecuteTask();
	void Evaluate(const WindowDistinctAggregatorGlobalState &gdstate, const DataChunk &bounds, Vector &result,
	              idx_t count, idx_t row_idx);

	//! Thread-local sorting data
	optional_ptr<LocalSortState> local_sort;
	//! Finalize stage
	PartitionSortStage stage = PartitionSortStage::INIT;
	//! Finalize scan block index
	idx_t block_idx;
	//! Thread-local tree aggregation
	Vector update_v;
	Vector source_v;
	Vector target_v;
	DataChunk leaves;
	SelectionVector sel;

protected:
	//! Flush the accumulated intermediate states into the result states
	void FlushStates();

	//! The aggregator we are working with
	const WindowDistinctAggregatorGlobalState &gastate;
	DataChunk sort_chunk;
	DataChunk payload_chunk;
	//! Reused result state container for the window functions
	WindowAggregateStates statef;
	//! A vector of pointers to "state", used for buffering intermediate aggregates
	Vector statep;
	//! Reused state pointers for combining tree elements
	Vector statel;
	//! Count of buffered values
	idx_t flush_count;
	//! The frame boundaries, used for the window functions
	SubFrames frames;
};

WindowDistinctAggregatorLocalState::WindowDistinctAggregatorLocalState(
    const WindowDistinctAggregatorGlobalState &gastate)
    : update_v(LogicalType::POINTER), source_v(LogicalType::POINTER), target_v(LogicalType::POINTER), gastate(gastate),
      statef(gastate.aggr), statep(LogicalType::POINTER), statel(LogicalType::POINTER), flush_count(0) {
	InitSubFrames(frames, gastate.aggregator.exclude_mode);
	payload_chunk.Initialize(Allocator::DefaultAllocator(), gastate.payload_types);

	sort_chunk.Initialize(Allocator::DefaultAllocator(), gastate.sort_types);
	sort_chunk.data.back().Reference(payload_chunk.data[0]);

	gastate.locals++;
}

unique_ptr<WindowAggregatorState> WindowDistinctAggregator::GetGlobalState(ClientContext &context, idx_t group_count,
                                                                           const ValidityMask &partition_mask) const {
	return make_uniq<WindowDistinctAggregatorGlobalState>(context, *this, group_count);
}

void WindowDistinctAggregator::Sink(WindowAggregatorState &gsink, WindowAggregatorState &lstate, DataChunk &sink_chunk,
                                    DataChunk &coll_chunk, idx_t input_idx, optional_ptr<SelectionVector> filter_sel,
                                    idx_t filtered) {
	WindowAggregator::Sink(gsink, lstate, sink_chunk, coll_chunk, input_idx, filter_sel, filtered);

	auto &ldstate = lstate.Cast<WindowDistinctAggregatorLocalState>();
	ldstate.Sink(sink_chunk, coll_chunk, input_idx, filter_sel, filtered);
}

void WindowDistinctAggregatorLocalState::Sink(DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx,
                                              optional_ptr<SelectionVector> filter_sel, idx_t filtered) {
	//	3: 	for i ← 0 to in.size do
	//	4: 		sorted[i] ← (in[i], i)
	const auto count = sink_chunk.size();
	payload_chunk.Reset();
	auto &sorted_vec = payload_chunk.data[0];
	auto sorted = FlatVector::GetData<idx_t>(sorted_vec);
	std::iota(sorted, sorted + count, input_idx);

	// Our arguments are being fully materialised,
	// but we also need them as sort keys.
	auto &child_idx = gastate.aggregator.child_idx;
	for (column_t c = 0; c < child_idx.size(); ++c) {
		sort_chunk.data[c].Reference(coll_chunk.data[child_idx[c]]);
	}
	sort_chunk.data.back().Reference(sorted_vec);
	sort_chunk.SetCardinality(sink_chunk);
	payload_chunk.SetCardinality(sort_chunk);

	//	Apply FILTER clause, if any
	if (filter_sel) {
		sort_chunk.Slice(*filter_sel, filtered);
		payload_chunk.Slice(*filter_sel, filtered);
	}

	if (!local_sort) {
		local_sort = gastate.InitializeLocalSort();
	}

	local_sort->SinkChunk(sort_chunk, payload_chunk);

	if (local_sort->SizeInBytes() > gastate.memory_per_thread) {
		local_sort->Sort(*gastate.global_sort, true);
	}
}

void WindowDistinctAggregatorLocalState::Finalize(WindowAggregatorGlobalState &gastate, CollectionPtr collection) {
	WindowAggregatorLocalState::Finalize(gastate, collection);

	//! Input data chunk, used for leaf segment aggregation
	leaves.Initialize(Allocator::DefaultAllocator(), cursor->chunk.GetTypes());
	sel.Initialize();
}

void WindowDistinctAggregatorLocalState::ExecuteTask() {
	auto &global_sort = *gastate.global_sort;
	switch (stage) {
	case PartitionSortStage::SCAN:
		global_sort.AddLocalState(*gastate.local_sorts[block_idx]);
		break;
	case PartitionSortStage::MERGE: {
		MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
		merge_sorter.PerformInMergeRound();
		break;
	}
	case PartitionSortStage::SORTED:
		Sorted();
		break;
	default:
		break;
	}

	++gastate.tasks_completed;
}

void WindowDistinctAggregatorGlobalState::MeasurePayloadBlocks() {
	const auto &blocks = global_sort->sorted_blocks[0]->payload_data->data_blocks;
	idx_t count = 0;
	for (const auto &block : blocks) {
		block_starts.emplace_back(count);
		count += block->count;
	}
	block_starts.emplace_back(count);
}

bool WindowDistinctAggregatorGlobalState::TryPrepareNextStage(WindowDistinctAggregatorLocalState &lstate) {
	lock_guard<mutex> stage_guard(lock);

	switch (stage.load()) {
	case PartitionSortStage::INIT:
		//	5: Sort sorted lexicographically increasing
		total_tasks = local_sorts.size();
		tasks_assigned = 0;
		tasks_completed = 0;
		lstate.stage = stage = PartitionSortStage::SCAN;
		lstate.block_idx = tasks_assigned++;
		return true;
	case PartitionSortStage::SCAN:
		// Process all the local sorts
		if (tasks_assigned < total_tasks) {
			lstate.stage = PartitionSortStage::SCAN;
			lstate.block_idx = tasks_assigned++;
			return true;
		} else if (tasks_completed < tasks_assigned) {
			return false;
		}
		global_sort->PrepareMergePhase();
		if (!(global_sort->sorted_blocks.size() / 2)) {
			if (global_sort->sorted_blocks.empty()) {
				lstate.stage = stage = PartitionSortStage::FINISHED;
				return true;
			}
			MeasurePayloadBlocks();
			seconds.resize(block_starts.size() - 1);
			total_tasks = seconds.size();
			tasks_completed = 0;
			tasks_assigned = 0;
			lstate.stage = stage = PartitionSortStage::SORTED;
			lstate.block_idx = tasks_assigned++;
			return true;
		}
		global_sort->InitializeMergeRound();
		lstate.stage = stage = PartitionSortStage::MERGE;
		total_tasks = locals;
		tasks_assigned = 1;
		tasks_completed = 0;
		return true;
	case PartitionSortStage::MERGE:
		if (tasks_assigned < total_tasks) {
			lstate.stage = PartitionSortStage::MERGE;
			++tasks_assigned;
			return true;
		} else if (tasks_completed < tasks_assigned) {
			return false;
		}
		global_sort->CompleteMergeRound(true);
		if (!(global_sort->sorted_blocks.size() / 2)) {
			MeasurePayloadBlocks();
			seconds.resize(block_starts.size() - 1);
			total_tasks = seconds.size();
			tasks_completed = 0;
			tasks_assigned = 0;
			lstate.stage = stage = PartitionSortStage::SORTED;
			lstate.block_idx = tasks_assigned++;
			return true;
		}
		global_sort->InitializeMergeRound();
		lstate.stage = PartitionSortStage::MERGE;
		total_tasks = locals;
		tasks_assigned = 1;
		tasks_completed = 0;
		return true;
	case PartitionSortStage::SORTED:
		if (tasks_assigned < total_tasks) {
			lstate.stage = PartitionSortStage::SORTED;
			lstate.block_idx = tasks_assigned++;
			return true;
		} else if (tasks_completed < tasks_assigned) {
			lstate.stage = PartitionSortStage::FINISHED;
			// Sleep while other tasks finish
			return false;
		}
		// Last task patches the boundaries
		PatchPrevIdcs();
		break;
	default:
		break;
	}

	lstate.stage = stage = PartitionSortStage::FINISHED;

	return true;
}

void WindowDistinctAggregator::Finalize(WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                        CollectionPtr collection, const FrameStats &stats) {
	auto &gdsink = gsink.Cast<WindowDistinctAggregatorGlobalState>();
	auto &ldstate = lstate.Cast<WindowDistinctAggregatorLocalState>();
	ldstate.Finalize(gdsink, collection);

	// Sort, merge and build the tree in parallel
	while (gdsink.stage.load() != PartitionSortStage::FINISHED) {
		if (gdsink.TryPrepareNextStage(ldstate)) {
			ldstate.ExecuteTask();
		} else {
			std::this_thread::yield();
		}
	}

	//	These are a parallel implementations,
	//	so every thread can call them.
	gdsink.zipped_tree.Build();
	gdsink.merge_sort_tree.Build(ldstate);

	++gdsink.finalized;
}

void WindowDistinctAggregatorLocalState::Sorted() {
	using ZippedTuple = WindowDistinctAggregatorGlobalState::ZippedTuple;
	auto &global_sort = gastate.global_sort;
	auto &prev_idcs = gastate.zipped_tree.LowestLevel();
	auto &aggregator = gastate.aggregator;
	auto &scan_chunk = payload_chunk;

	auto scanner = make_uniq<PayloadScanner>(*global_sort, block_idx);
	const auto in_size = gastate.block_starts.at(block_idx + 1);
	scanner->Scan(scan_chunk);
	idx_t scan_idx = 0;

	auto *input_idx = FlatVector::GetData<idx_t>(scan_chunk.data[0]);
	idx_t i = 0;

	SBIterator curr(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator prev(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	auto prefix_layout = global_sort->sort_layout.GetPrefixComparisonLayout(aggregator.arg_types.size());

	const auto block_begin = gastate.block_starts.at(block_idx);
	if (!block_begin) {
		// First block, so set up initial sentinel
		i = input_idx[scan_idx++];
		prev_idcs[i] = ZippedTuple(0, i);
		std::get<0>(gastate.seconds[block_idx]) = i;
	} else {
		// Move to the to end of the previous block
		// so we can record the comparison result for the first row
		curr.SetIndex(block_begin - 1);
		prev.SetIndex(block_begin - 1);
		scan_idx = 0;
		std::get<0>(gastate.seconds[block_idx]) = input_idx[scan_idx];
	}

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

	// Save the last value of i for patching up the block boundaries
	std::get<1>(gastate.seconds[block_idx]) = i;
}

void WindowDistinctAggregatorGlobalState::PatchPrevIdcs() {
	//	13:	return prevIdcs

	// Patch up the indices at block boundaries
	// (We don't need to patch block 0.)
	auto &prev_idcs = zipped_tree.LowestLevel();
	for (idx_t block_idx = 1; block_idx < seconds.size(); ++block_idx) {
		// We only need to patch if the first index in the block
		// was a back link to the previous block (10:)
		auto i = std::get<0>(seconds.at(block_idx));
		if (std::get<0>(prev_idcs[i])) {
			auto second = std::get<1>(seconds.at(block_idx - 1));
			prev_idcs[i] = ZippedTuple(second + 1, i);
		}
	}
}

bool WindowDistinctSortTree::TryNextRun(idx_t &level_idx, idx_t &run_idx) {
	const auto fanout = FANOUT;

	lock_guard<mutex> stage_guard(build_lock);

	//	Verify we are not done
	if (build_level >= tree.size()) {
		return false;
	}

	// Finished with this level?
	if (build_complete >= build_num_runs) {
		auto &zipped_tree = gdastate.zipped_tree;
		std::swap(tree[build_level].second, zipped_tree.tree[build_level].second);

		++build_level;
		if (build_level >= tree.size()) {
			zipped_tree.tree.clear();
			return false;
		}

		const auto count = LowestLevel().size();
		build_run_length *= fanout;
		build_num_runs = (count + build_run_length - 1) / build_run_length;
		build_run = 0;
		build_complete = 0;
	}

	// If all runs are in flight,
	// yield until the next level is ready
	if (build_run >= build_num_runs) {
		return false;
	}

	level_idx = build_level;
	run_idx = build_run++;

	return true;
}

void WindowDistinctSortTree::Build(WindowDistinctAggregatorLocalState &ldastate) {
	//	Fan in parent levels until we are at the top
	//	Note that we don't build the top layer as that would just be all the data.
	while (build_level.load() < tree.size()) {
		idx_t level_idx;
		idx_t run_idx;
		if (TryNextRun(level_idx, run_idx)) {
			BuildRun(level_idx, run_idx, ldastate);
		} else {
			std::this_thread::yield();
		}
	}
}

void WindowDistinctSortTree::BuildRun(idx_t level_nr, idx_t run_idx, WindowDistinctAggregatorLocalState &ldastate) {
	auto &aggr = gdastate.aggr;
	auto &allocator = gdastate.allocator;
	auto &inputs = ldastate.cursor->chunk;
	auto &levels_flat_native = gdastate.levels_flat_native;

	//! Input data chunk, used for leaf segment aggregation
	auto &leaves = ldastate.leaves;
	auto &sel = ldastate.sel;

	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);

	//! The states to update
	auto &update_v = ldastate.update_v;
	auto updates = FlatVector::GetData<data_ptr_t>(update_v);

	auto &source_v = ldastate.source_v;
	auto sources = FlatVector::GetData<data_ptr_t>(source_v);
	auto &target_v = ldastate.target_v;
	auto targets = FlatVector::GetData<data_ptr_t>(target_v);

	auto &zipped_tree = gdastate.zipped_tree;
	auto &zipped_level = zipped_tree.tree[level_nr].first;
	auto &level = tree[level_nr].first;

	//	Reset the combine state
	idx_t nupdate = 0;
	idx_t ncombine = 0;
	data_ptr_t prev_state = nullptr;
	idx_t i = run_idx * build_run_length;
	auto next_limit = MinValue<idx_t>(zipped_level.size(), i + build_run_length);
	idx_t levels_flat_offset = level_nr * zipped_level.size() + i;
	for (auto j = i; j < next_limit; ++j) {
		//	Initialise the next aggregate
		auto curr_state = levels_flat_native.GetStatePtr(levels_flat_offset++);

		//	Update this state (if it matches)
		const auto prev_idx = std::get<0>(zipped_level[j]);
		level[j] = prev_idx;
		if (prev_idx < i + 1) {
			const auto update_idx = std::get<1>(zipped_level[j]);
			if (!ldastate.cursor->RowIsVisible(update_idx)) {
				// 	Flush if we have to move the cursor
				//	Push the updates first so they propagate
				leaves.Reference(inputs);
				leaves.Slice(sel, nupdate);
				aggr.function.update(leaves.data.data(), aggr_input_data, leaves.ColumnCount(), update_v, nupdate);
				nupdate = 0;

				//	Combine the states sequentially
				aggr.function.combine(source_v, target_v, aggr_input_data, ncombine);
				ncombine = 0;

				// Move the update into range.
				ldastate.cursor->Seek(update_idx);
			}

			updates[nupdate] = curr_state;
			//	input_idx
			sel[nupdate] = ldastate.cursor->RowOffset(update_idx);
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

	++build_complete;
}

void WindowDistinctAggregatorLocalState::FlushStates() {
	if (!flush_count) {
		return;
	}

	const auto &aggr = gastate.aggr;
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	statel.Verify(flush_count);
	aggr.function.combine(statel, statep, aggr_input_data, flush_count);

	flush_count = 0;
}

void WindowDistinctAggregatorLocalState::Evaluate(const WindowDistinctAggregatorGlobalState &gdstate,
                                                  const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) {
	auto ldata = FlatVector::GetData<const_data_ptr_t>(statel);
	auto pdata = FlatVector::GetData<data_ptr_t>(statep);

	const auto &merge_sort_tree = gdstate.merge_sort_tree;
	const auto &levels_flat_native = gdstate.levels_flat_native;
	const auto exclude_mode = gdstate.aggregator.exclude_mode;

	//	Build the finalise vector that just points to the result states
	statef.Initialize(count);

	WindowAggregator::EvaluateSubFrames(bounds, exclude_mode, count, row_idx, frames, [&](idx_t rid) {
		auto agg_state = statef.GetStatePtr(rid);

		//	TODO: Extend AggregateLowerBound to handle subframes, just like SelectNth.
		const auto lower = frames[0].start;
		const auto upper = frames[0].end;
		merge_sort_tree.AggregateLowerBound(lower, upper, lower + 1,
		                                    [&](idx_t level, const idx_t run_begin, const idx_t run_pos) {
			                                    if (run_pos != run_begin) {
				                                    //	Find the source aggregate
				                                    // Buffer a merge of the indicated state into the current state
				                                    const auto agg_idx = gdstate.levels_flat_start[level] + run_pos - 1;
				                                    const auto running_agg = levels_flat_native.GetStatePtr(agg_idx);
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
	statef.Finalize(result);

	//	Destruct any non-POD state
	statef.Destroy();
}

unique_ptr<WindowAggregatorState> WindowDistinctAggregator::GetLocalState(const WindowAggregatorState &gstate) const {
	return make_uniq<WindowDistinctAggregatorLocalState>(gstate.Cast<const WindowDistinctAggregatorGlobalState>());
}

void WindowDistinctAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                        const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {

	const auto &gdstate = gsink.Cast<WindowDistinctAggregatorGlobalState>();
	auto &ldstate = lstate.Cast<WindowDistinctAggregatorLocalState>();
	ldstate.Evaluate(gdstate, bounds, result, count, row_idx);
}

} // namespace duckdb
