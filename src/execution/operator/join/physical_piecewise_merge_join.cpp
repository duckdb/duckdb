#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalPiecewiseMergeJoin::PhysicalPiecewiseMergeJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                       unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
                                                       JoinType join_type, idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::PIECEWISE_MERGE_JOIN, move(cond), join_type,
                             estimated_cardinality) {
	// for now we only support one condition!
	D_ASSERT(conditions.size() == 1);
	for (auto &cond : conditions) {
		D_ASSERT(cond.left->return_type == cond.right->return_type);
		join_key_types.push_back(cond.left->return_type);

		// Convert the conditions to sort orders
		auto left = cond.left->Copy();
		auto right = cond.right->Copy();
		switch (cond.comparison) {
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			lhs_orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, move(left)));
			rhs_orders.emplace_back(BoundOrderByNode(OrderType::ASCENDING, OrderByNullType::NULLS_LAST, move(right)));
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			lhs_orders.emplace_back(BoundOrderByNode(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, move(left)));
			rhs_orders.emplace_back(BoundOrderByNode(OrderType::DESCENDING, OrderByNullType::NULLS_LAST, move(right)));
			break;
		default:
			// COMPARE NOT EQUAL not supported with merge join
			throw NotImplementedException("Unimplemented join type for merge join");
		}
	}
	children.push_back(move(left));
	children.push_back(move(right));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MergeJoinGlobalState : public GlobalSinkState {
public:
	MergeJoinGlobalState(BufferManager &buffer_manager, const vector<BoundOrderByNode> &orders, RowLayout &rhs_layout)
	    : rhs_global_sort_state(buffer_manager, orders, rhs_layout), rhs_has_null(0), rhs_count(0),
	      memory_per_thread(0) {
	}

	inline idx_t Count() const {
		return rhs_count;
	}

	//! The lock for updating the global state
	mutex lock;
	//! Global sort state
	GlobalSortState rhs_global_sort_state;
	//! Whether or not the RHS has NULL values
	idx_t rhs_has_null;
	//! The total number of rows in the RHS
	idx_t rhs_count;
	//! A bool indicating for each tuple in the RHS if they found a match (only used in FULL OUTER JOIN)
	unique_ptr<bool[]> rhs_found_match;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

unique_ptr<GlobalSinkState> PhysicalPiecewiseMergeJoin::GetGlobalSinkState(ClientContext &context) const {
	// Get the payload layout from the rhs types
	RowLayout rhs_layout;
	auto types = children[1]->types;
	rhs_layout.Initialize(types);
	auto state = make_unique<MergeJoinGlobalState>(BufferManager::GetBufferManager(context), rhs_orders, rhs_layout);
	// Set external (can be force with the PRAGMA)
	auto &config = ClientConfig::GetConfig(context);
	state->rhs_global_sort_state.external = config.force_external;
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/4th of this, to be conservative
	idx_t max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	state->memory_per_thread = (max_memory / num_threads) / 4;
	return move(state);
}

static idx_t CountValid(Vector &v, const idx_t count) {
	idx_t valid = 0;

	VectorData vdata;
	v.Orrify(count, vdata);
	if (vdata.validity.AllValid()) {
		return count;
	}
	switch (v.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		valid += vdata.validity.CountValid(count);
		break;
	case VectorType::CONSTANT_VECTOR:
		valid += vdata.validity.CountValid(1) * count;
		break;
	default:
		for (idx_t i = 0; i < count; ++i) {
			const auto row_idx = vdata.sel->get_index(i);
			valid += int(vdata.validity.RowIsValid(row_idx));
		}
		break;
	}

	return valid;
}

class MergeJoinLocalState : public LocalSinkState {
public:
	explicit MergeJoinLocalState() : rhs_has_null(0), rhs_count(0) {
	}

	//! The local sort state
	LocalSortState rhs_local_sort_state;
	//! Local copy of the sorting expression executor
	ExpressionExecutor rhs_executor;
	//! Holds a vector of incoming sorting columns
	DataChunk rhs_keys;
	//! Whether or not the RHS has NULL values
	idx_t rhs_has_null;
	//! The total number of rows in the RHS
	idx_t rhs_count;
};

unique_ptr<LocalSinkState> PhysicalPiecewiseMergeJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_unique<MergeJoinLocalState>();
	// Initialize order clause expression executor and DataChunk
	vector<LogicalType> types;
	for (auto &order : rhs_orders) {
		types.push_back(order.expression->return_type);
		result->rhs_executor.AddExpression(*order.expression);
	}
	result->rhs_keys.Initialize(types);
	return move(result);
}

SinkResultType PhysicalPiecewiseMergeJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                LocalSinkState &lstate_p, DataChunk &input) const {
	auto &gstate = (MergeJoinGlobalState &)gstate_p;
	auto &lstate = (MergeJoinLocalState &)lstate_p;

	auto &global_sort_state = gstate.rhs_global_sort_state;
	auto &local_sort_state = lstate.rhs_local_sort_state;

	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, BufferManager::GetBufferManager(context.client));
	}

	// Obtain sorting columns
	auto &join_keys = lstate.rhs_keys;
	join_keys.Reset();
	lstate.rhs_executor.Execute(input, join_keys);

	// Count the NULLs so we can exclude them later
	// TODO: Sort any comparison NULLs to the end using an initial sort column
	const auto count = join_keys.size();
	for (auto &key : join_keys.data) {
		lstate.rhs_has_null += (count - CountValid(key, count));
	}
	lstate.rhs_count += count;

	// Sink the data into the local sort state
	local_sort_state.SinkChunk(join_keys, input);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalPiecewiseMergeJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                         LocalSinkState &lstate_p) const {
	auto &gstate = (MergeJoinGlobalState &)gstate_p;
	auto &lstate = (MergeJoinLocalState &)lstate_p;
	gstate.rhs_global_sort_state.AddLocalState(lstate.rhs_local_sort_state);
	lock_guard<mutex> locked(gstate.lock);
	gstate.rhs_has_null += lstate.rhs_has_null;
	gstate.rhs_count += lstate.rhs_count;
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(this, &lstate.rhs_executor, "rhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class MergeJoinFinalizeTask : public ExecutorTask {
public:
	MergeJoinFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, MergeJoinGlobalState &state)
	    : ExecutorTask(context), event(move(event_p)), context(context), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize merge sorted and iterate until done
		auto &global_sort_state = state.rhs_global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();

		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	MergeJoinGlobalState &state;
};

class MergeJoinFinalizeEvent : public Event {
public:
	MergeJoinFinalizeEvent(MergeJoinGlobalState &gstate_p, Pipeline &pipeline_p)
	    : Event(pipeline_p.executor), gstate(gstate_p), pipeline(pipeline_p) {
	}

	MergeJoinGlobalState &gstate;
	Pipeline &pipeline;

public:
	void Schedule() override {
		auto &context = pipeline.GetClientContext();

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();

		vector<unique_ptr<Task>> merge_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			merge_tasks.push_back(make_unique<MergeJoinFinalizeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(move(merge_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.rhs_global_sort_state;

		global_sort_state.CompleteMergeRound(true);
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalPiecewiseMergeJoin::ScheduleMergeTasks(pipeline, *this, gstate);
		}
	}
};

void PhysicalPiecewiseMergeJoin::ScheduleMergeTasks(Pipeline &pipeline, Event &event, MergeJoinGlobalState &gstate) {
	// Initialize global sort state for a round of merging
	gstate.rhs_global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<MergeJoinFinalizeEvent>(gstate, pipeline);
	event.InsertEvent(move(new_event));
}

SinkFinalizeType PhysicalPiecewiseMergeJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      GlobalSinkState &gstate_p) const {
	auto &gstate = (MergeJoinGlobalState &)gstate_p;
	auto &global_sort_state = gstate.rhs_global_sort_state;

	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER JOIN, initialize found_match to false for every tuple
		gstate.rhs_found_match = unique_ptr<bool[]>(new bool[gstate.Count()]);
		memset(gstate.rhs_found_match.get(), 0, sizeof(bool) * gstate.Count());
	}
	if (global_sort_state.sorted_blocks.empty() && EmptyResultIfRHSIsEmpty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalPiecewiseMergeJoin::ScheduleMergeTasks(pipeline, event, gstate);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class PiecewiseMergeJoinState : public OperatorState {
public:
	explicit PiecewiseMergeJoinState(const PhysicalPiecewiseMergeJoin &op, BufferManager &buffer_manager,
	                                 bool force_external)
	    : op(op), buffer_manager(buffer_manager), force_external(force_external), left_position(0), first_fetch(true),
	      finished(true), right_position(0), right_chunk_index(0) {
		vector<LogicalType> condition_types;
		for (auto &cond : op.conditions) {
			lhs_executor.AddExpression(*cond.left);
			condition_types.push_back(cond.left->return_type);
		}
		lhs_keys.Initialize(condition_types);
		if (IsLeftOuterJoin(op.join_type)) {
			lhs_found_match = unique_ptr<bool[]>(new bool[STANDARD_VECTOR_SIZE]);
			memset(lhs_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
		}
		lhs_layout.Initialize(op.children[0]->types);
	}

	const PhysicalPiecewiseMergeJoin &op;
	BufferManager &buffer_manager;
	bool force_external;

	// Block sorting
	DataChunk lhs_keys;
	ExpressionExecutor lhs_executor;
	unique_ptr<bool[]> lhs_found_match;
	RowLayout lhs_layout;
	unique_ptr<LocalSortState> lhs_local_state;
	unique_ptr<GlobalSortState> lhs_global_state;
	idx_t lhs_count;
	idx_t lhs_has_null;

	// Simple scans
	idx_t left_position;

	// Complex scans
	bool first_fetch;
	bool finished;
	idx_t right_position;
	idx_t right_chunk_index;
	idx_t right_base;

public:
	void ResolveJoinKeys(DataChunk &input) {
		// resolve the join keys for the input
		lhs_keys.Reset();
		lhs_executor.Execute(input, lhs_keys);

		// Count the NULLs so we can exclude them later
		// TODO: Sort any multi-comparison NULLs to the end using an initial sort column
		lhs_count = lhs_keys.size();
		for (auto &key : lhs_keys.data) {
			lhs_has_null = lhs_count - CountValid(key, lhs_count);
			break;
		}

		// sort by join key
		lhs_global_state = make_unique<GlobalSortState>(buffer_manager, op.lhs_orders, lhs_layout);
		lhs_local_state = make_unique<LocalSortState>();
		lhs_local_state->Initialize(*lhs_global_state, buffer_manager);
		lhs_local_state->SinkChunk(lhs_keys, input);

		// Set external (can be force with the PRAGMA)
		lhs_global_state->external = force_external;
		lhs_global_state->AddLocalState(*lhs_local_state);
		lhs_global_state->PrepareMergePhase();
		while (lhs_global_state->sorted_blocks.size() > 1) {
			MergeSorter merge_sorter(*lhs_global_state, buffer_manager);
			merge_sorter.PerformInMergeRound();
			lhs_global_state->CompleteMergeRound();
		}
	}

	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &lhs_executor, "lhs_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalPiecewiseMergeJoin::GetOperatorState(ClientContext &context) const {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &config = ClientConfig::GetConfig(context);
	return make_unique<PiecewiseMergeJoinState>(*this, buffer_manager, config.force_external);
}

static inline idx_t SortedBlockNotNull(const idx_t base, const idx_t count, const idx_t not_null) {
	return MinValue(base + count, MaxValue(base, not_null)) - base;
}

static int MergeJoinComparisonValue(ExpressionType comparison) {
	switch (comparison) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		return -1;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return 0;
	default:
		throw InternalException("Unimplemented comparison type for merge join!");
	}
}

struct BlockMergeInfo {
	GlobalSortState &state;
	//! The block being scanned
	const idx_t block_idx;
	//! The start position being read from the block
	const idx_t base_idx;
	//! The number of not-NULL values in the block (they are at the end)
	const idx_t not_null;
	//! The current offset in the block
	idx_t &entry_idx;
	SelectionVector result;

	BlockMergeInfo(GlobalSortState &state, idx_t block_idx, idx_t base_idx, idx_t &entry_idx, idx_t not_null)
	    : state(state), block_idx(block_idx), base_idx(base_idx), not_null(not_null), entry_idx(entry_idx),
	      result(STANDARD_VECTOR_SIZE) {
	}
};

static void SliceSortedPayload(DataChunk &payload, BlockMergeInfo &info, const idx_t result_count, const idx_t vcount,
                               const idx_t left_cols = 0) {
	// There should only be one sorted block if they have been sorted
	D_ASSERT(info.state.sorted_blocks.size() == 1);
	SBScanState read_state(info.state.buffer_manager, info.state);
	read_state.sb = info.state.sorted_blocks[0].get();
	auto &sorted_data = *read_state.sb->payload_data;

	// We have to create pointers for the entire block
	// because unswizzle works on ranges not selections.
	read_state.SetIndices(info.block_idx, info.base_idx);
	read_state.PinData(sorted_data);
	const auto data_ptr = read_state.DataPtr(sorted_data);
	const auto next = vcount - info.base_idx;

	// Set up a batch of pointers to scan data from
	Vector addresses(LogicalType::POINTER, next);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// Set up the data pointers
	data_ptr_t row_ptr = data_ptr;
	const idx_t &row_width = sorted_data.layout.GetRowWidth();
	for (idx_t i = 0; i < next; ++i) {
		data_pointers[i] = row_ptr;
		row_ptr += row_width;
	}
	// Unswizzle the offsets back to pointers (if needed)
	if (!sorted_data.layout.AllConstant() && info.state.external) {
		RowOperations::UnswizzlePointers(sorted_data.layout, data_ptr, read_state.payload_heap_handle->Ptr(), next);
	}

	// Deserialize the payload data
	for (idx_t col_idx = 0; col_idx < sorted_data.layout.ColumnCount(); col_idx++) {
		const auto col_offset = sorted_data.layout.GetOffsets()[col_idx];
		RowOperations::Gather(addresses, info.result, payload.data[left_cols + col_idx],
		                      *FlatVector::IncrementalSelectionVector(), result_count, col_offset, col_idx);
	}
}

static void MergeJoinPinSortingBlock(SBScanState &scan, const idx_t block_idx) {
	scan.SetIndices(block_idx, 0);
	scan.PinRadix(block_idx);

	auto &sd = *scan.sb->blob_sorting_data;
	if (block_idx < sd.data_blocks.size()) {
		scan.PinData(sd);
	}
}

static data_ptr_t MergeJoinRadixPtr(SBScanState &scan, const idx_t entry_idx) {
	scan.entry_idx = entry_idx;
	return scan.RadixPtr();
}

static idx_t MergeJoinSimpleBlocks(PiecewiseMergeJoinState &lstate, MergeJoinGlobalState &rstate, bool *found_match,
                                   const ExpressionType comparison) {
	const auto cmp = MergeJoinComparisonValue(comparison);

	// The sort parameters should all be the same
	auto &lsort = *lstate.lhs_global_state;
	auto &rsort = rstate.rhs_global_sort_state;
	D_ASSERT(lsort.sort_layout.all_constant == rsort.sort_layout.all_constant);
	const auto all_constant = lsort.sort_layout.all_constant;
	D_ASSERT(lsort.external == rsort.external);
	const auto external = lsort.external;

	// There should only be one sorted block if they have been sorted
	D_ASSERT(lsort.sorted_blocks.size() == 1);
	SBScanState lread(lsort.buffer_manager, lsort);
	lread.sb = lsort.sorted_blocks[0].get();

	const idx_t l_block_idx = 0;
	idx_t l_entry_idx = 0;
	const auto lhs_not_null = lstate.lhs_count - lstate.lhs_has_null;
	MergeJoinPinSortingBlock(lread, l_block_idx);
	auto l_ptr = MergeJoinRadixPtr(lread, l_entry_idx);

	D_ASSERT(rsort.sorted_blocks.size() == 1);
	SBScanState rread(rsort.buffer_manager, rsort);
	rread.sb = rsort.sorted_blocks[0].get();

	const auto cmp_size = lsort.sort_layout.comparison_size;
	const auto entry_size = lsort.sort_layout.entry_size;

	idx_t right_base = 0;
	for (idx_t r_block_idx = 0; r_block_idx < rread.sb->radix_sorting_data.size(); r_block_idx++) {
		// we only care about the BIGGEST value in each of the RHS data blocks
		// because we want to figure out if the LHS values are less than [or equal] to ANY value
		// get the biggest value from the RHS chunk
		MergeJoinPinSortingBlock(rread, r_block_idx);

		auto &rblock = rread.sb->radix_sorting_data[r_block_idx];
		const auto r_not_null = SortedBlockNotNull(right_base, rblock.count, rstate.rhs_count - rstate.rhs_has_null);
		if (r_not_null == 0) {
			break;
		}
		const auto r_entry_idx = r_not_null - 1;
		right_base += rblock.count;

		auto r_ptr = MergeJoinRadixPtr(rread, r_entry_idx);

		// now we start from the current lpos value and check if we found a new value that is [<= OR <] the max RHS
		// value
		while (true) {
			int comp_res;
			if (all_constant) {
				comp_res = FastMemcmp(l_ptr, r_ptr, cmp_size);
			} else {
				comp_res = Comparators::CompareTuple(lread, rread, l_ptr, r_ptr, lsort.sort_layout, external);
			}

			if (comp_res <= cmp) {
				// found a match for lpos, set it in the found_match vector
				found_match[l_entry_idx] = true;
				l_entry_idx++;
				l_ptr += entry_size;
				if (l_entry_idx >= lhs_not_null) {
					// early out: we exhausted the entire LHS and they all match
					return 0;
				}
			} else {
				// we found no match: any subsequent value from the LHS we scan now will be bigger and thus also not
				// match move to the next RHS chunk
				break;
			}
		}
	}
	return 0;
}

void PhysicalPiecewiseMergeJoin::ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   OperatorState &state_p) const {
	auto &state = (PiecewiseMergeJoinState &)state_p;
	auto &gstate = (MergeJoinGlobalState &)*sink_state;

	state.ResolveJoinKeys(input);

	// perform the actual join
	bool found_match[STANDARD_VECTOR_SIZE];
	memset(found_match, 0, sizeof(found_match));
	MergeJoinSimpleBlocks(state, gstate, found_match, conditions[0].comparison);

	// extract the sorted payload
	idx_t left_position = 0;
	const auto lhs_not_null = state.lhs_count - state.lhs_has_null;
	BlockMergeInfo left_info(*state.lhs_global_state, 0, 0, left_position, lhs_not_null);
	left_info.result.Initialize(nullptr);

	DataChunk payload;
	payload.Initialize(input.GetTypes());
	SliceSortedPayload(payload, left_info, state.lhs_count, state.lhs_count);
	payload.SetCardinality(state.lhs_count);

	// now construct the result based on the join result
	switch (join_type) {
	case JoinType::MARK: {
		// The only part of the join keys that is actually used is the validity mask.
		// Since the payload is sorted, we can just set the tail end of the validity masks to invalid.
		for (auto &key : state.lhs_keys.data) {
			key.Normalify(state.lhs_keys.size());
			auto &mask = FlatVector::Validity(key);
			if (mask.AllValid()) {
				continue;
			}
			mask.SetAllValid(lhs_not_null);
			for (idx_t i = lhs_not_null; i < state.lhs_count; ++i) {
				mask.SetInvalid(i);
			}
		}
		// So we make a set of keys that have the validity mask set for the
		PhysicalJoin::ConstructMarkJoinResult(state.lhs_keys, payload, chunk, found_match, gstate.rhs_has_null);
		break;
	}
	case JoinType::SEMI:
		PhysicalJoin::ConstructSemiJoinResult(payload, chunk, found_match);
		break;
	case JoinType::ANTI:
		PhysicalJoin::ConstructAntiJoinResult(payload, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented join type for merge join");
	}
}

static idx_t MergeJoinComplexBlocks(BlockMergeInfo &l, BlockMergeInfo &r, const ExpressionType comparison) {
	const auto cmp = MergeJoinComparisonValue(comparison);

	// The sort parameters should all be the same
	D_ASSERT(l.state.sort_layout.all_constant == r.state.sort_layout.all_constant);
	const auto all_constant = r.state.sort_layout.all_constant;
	D_ASSERT(l.state.external == r.state.external);
	const auto external = l.state.external;

	// There should only be one sorted block if they have been sorted
	D_ASSERT(l.state.sorted_blocks.size() == 1);
	SBScanState lread(l.state.buffer_manager, l.state);
	lread.sb = l.state.sorted_blocks[0].get();
	D_ASSERT(lread.sb->radix_sorting_data.size() == 1);
	MergeJoinPinSortingBlock(lread, l.block_idx);
	auto l_start = MergeJoinRadixPtr(lread, 0);
	auto l_ptr = MergeJoinRadixPtr(lread, l.entry_idx);

	D_ASSERT(r.state.sorted_blocks.size() == 1);
	SBScanState rread(r.state.buffer_manager, r.state);
	rread.sb = r.state.sorted_blocks[0].get();

	if (r.entry_idx >= r.not_null) {
		return 0;
	}

	MergeJoinPinSortingBlock(rread, r.block_idx);
	auto r_ptr = MergeJoinRadixPtr(rread, r.entry_idx);

	const auto cmp_size = l.state.sort_layout.comparison_size;
	const auto entry_size = l.state.sort_layout.entry_size;

	idx_t result_count = 0;
	while (true) {
		if (l.entry_idx < l.not_null) {
			int comp_res;
			if (all_constant) {
				comp_res = FastMemcmp(l_ptr, r_ptr, cmp_size);
			} else {
				comp_res = Comparators::CompareTuple(lread, rread, l_ptr, r_ptr, l.state.sort_layout, external);
			}

			if (comp_res <= cmp) {
				// left side smaller: found match
				l.result.set_index(result_count, sel_t(l.entry_idx - l.base_idx));
				r.result.set_index(result_count, sel_t(r.entry_idx - r.base_idx));
				result_count++;
				// move left side forward
				l.entry_idx++;
				l_ptr += entry_size;
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					break;
				}
				continue;
			}
		}
		// right side smaller or equal, or left side exhausted: move
		// right pointer forward reset left side to start
		r.entry_idx++;
		if (r.entry_idx >= r.not_null) {
			break;
		}
		r_ptr += entry_size;

		l_ptr = l_start;
		l.entry_idx = 0;
	}

	return result_count;
}

OperatorResultType PhysicalPiecewiseMergeJoin::ResolveComplexJoin(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (PiecewiseMergeJoinState &)state_p;
	auto &gstate = (MergeJoinGlobalState &)*sink_state;
	auto &rsorted = *gstate.rhs_global_sort_state.sorted_blocks[0];
	do {
		if (state.first_fetch) {
			state.ResolveJoinKeys(input);

			state.right_chunk_index = 0;
			state.right_base = 0;
			state.left_position = 0;
			state.right_position = 0;
			state.first_fetch = false;
			state.finished = false;
		}
		if (state.finished) {
			if (IsLeftOuterJoin(join_type)) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				PhysicalJoin::ConstructLeftJoinResult(input, chunk, state.lhs_found_match.get());
				memset(state.lhs_found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
			}
			state.first_fetch = true;
			state.finished = false;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		const auto lhs_not_null = state.lhs_count - state.lhs_has_null;
		BlockMergeInfo left_info(*state.lhs_global_state, 0, 0, state.left_position, lhs_not_null);

		const auto &rblock = rsorted.radix_sorting_data[state.right_chunk_index];
		const auto rhs_not_null =
		    SortedBlockNotNull(state.right_base, rblock.count, gstate.rhs_count - gstate.rhs_has_null);
		BlockMergeInfo right_info(gstate.rhs_global_sort_state, state.right_chunk_index, state.right_position,
		                          state.right_position, rhs_not_null);

		idx_t result_count = MergeJoinComplexBlocks(left_info, right_info, conditions[0].comparison);
		if (result_count == 0) {
			// exhausted this chunk on the right side
			// move to the next right chunk
			state.left_position = 0;
			state.right_position = 0;
			state.right_base += rsorted.radix_sorting_data[state.right_chunk_index].count;
			state.right_chunk_index++;
			if (state.right_chunk_index >= rsorted.radix_sorting_data.size()) {
				state.finished = true;
			}
		} else {
			// found matches: mark the found matches if required
			if (state.lhs_found_match) {
				for (idx_t i = 0; i < result_count; i++) {
					state.lhs_found_match[left_info.result[i]] = true;
				}
			}
			if (gstate.rhs_found_match) {
				//	Absolute position of the block + start position inside that block
				const idx_t base_index = state.right_base + right_info.base_idx;
				for (idx_t i = 0; i < result_count; i++) {
					gstate.rhs_found_match[base_index + right_info.result[i]] = true;
				}
			}
			// found matches: output them
			SliceSortedPayload(chunk, left_info, result_count, input.size());
			SliceSortedPayload(chunk, right_info, result_count, right_info.entry_idx + 1, input.ColumnCount());
			chunk.SetCardinality(result_count);
			chunk.Verify();
		}
	} while (chunk.size() == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalPiecewiseMergeJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       OperatorState &state) const {
	auto &gstate = (MergeJoinGlobalState &)*sink_state;

	if (gstate.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.rhs_has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		// simple joins can have max STANDARD_VECTOR_SIZE matches per chunk
		ResolveSimpleJoin(context, input, chunk, state);
		return OperatorResultType::NEED_MORE_INPUT;
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		return ResolveComplexJoin(context, input, chunk, state);
	default:
		throw NotImplementedException("Unimplemented type for piecewise merge loop join!");
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PiecewiseJoinScanState : public GlobalSourceState {
public:
	explicit PiecewiseJoinScanState(const PhysicalPiecewiseMergeJoin &op) : op(op), right_outer_position(0) {
	}

	mutex lock;
	const PhysicalPiecewiseMergeJoin &op;
	unique_ptr<PayloadScanner> scanner;
	idx_t right_outer_position;

public:
	idx_t MaxThreads() override {
		auto &sink = (MergeJoinGlobalState &)*op.sink_state;
		return sink.Count() / (STANDARD_VECTOR_SIZE * idx_t(10));
	}
};

unique_ptr<GlobalSourceState> PhysicalPiecewiseMergeJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PiecewiseJoinScanState>(*this);
}

void PhysicalPiecewiseMergeJoin::GetData(ExecutionContext &context, DataChunk &result, GlobalSourceState &gstate,
                                         LocalSourceState &lstate) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (MergeJoinGlobalState &)*sink_state;
	auto &state = (PiecewiseJoinScanState &)gstate;

	lock_guard<mutex> l(state.lock);
	if (!state.scanner) {
		// Initialize scanner (if not yet initialized)
		auto &sort_state = sink.rhs_global_sort_state;
		if (sort_state.sorted_blocks.empty()) {
			return;
		}
		state.scanner = make_unique<PayloadScanner>(*sort_state.sorted_blocks[0]->payload_data, sort_state);
	}

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan the found_match for any chunks we
	// still need to output
	const auto found_match = sink.rhs_found_match.get();

	// ConstructFullOuterJoinResult(sink.rhs_found_match.get(), sink.right_chunks, chunk, state.right_outer_position);
	DataChunk rhs_chunk;
	rhs_chunk.Initialize(children[1]->types);
	SelectionVector rsel(STANDARD_VECTOR_SIZE);
	for (;;) {
		// Read the next sorted chunk
		state.scanner->Scan(rhs_chunk);

		const auto count = rhs_chunk.size();
		if (count == 0) {
			return;
		}

		idx_t result_count = 0;
		// figure out which tuples didn't find a match in the RHS
		for (idx_t i = 0; i < count; i++) {
			if (!found_match[state.right_outer_position + i]) {
				rsel.set_index(result_count++, i);
			}
		}
		state.right_outer_position += count;

		if (result_count > 0) {
			// if there were any tuples that didn't find a match, output them
			const idx_t left_column_count = result.ColumnCount() - rhs_chunk.ColumnCount();
			for (idx_t col_idx = 0; col_idx < left_column_count; ++col_idx) {
				result.data[col_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result.data[col_idx], true);
			}
			for (idx_t col_idx = 0; col_idx < rhs_chunk.ColumnCount(); ++col_idx) {
				result.data[left_column_count + col_idx].Slice(rhs_chunk.data[col_idx], rsel, result_count);
			}
			result.SetCardinality(result_count);
			return;
		}
	}
}

} // namespace duckdb
