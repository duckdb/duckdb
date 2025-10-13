#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/function/window/window_aggregate_function.hpp"
#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/function/window/window_rank_function.hpp"
#include "duckdb/function/window/window_rownumber_function.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window/window_value_function.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

//	Global sink state
class WindowGlobalSinkState;

enum WindowGroupStage : uint8_t { MASK, SINK, FINALIZE, GETDATA, DONE };

struct WindowSourceTask {
	WindowSourceTask() {
	}

	WindowGroupStage stage = WindowGroupStage::DONE;
	//! The hash group
	idx_t group_idx = 0;
	//! The thread index (for local state)
	idx_t thread_idx = 0;
	//! The total block index count
	idx_t max_idx = 0;
	//! The first block index count
	idx_t begin_idx = 0;
	//! The end block index count
	idx_t end_idx = 0;
};

class WindowHashGroup {
public:
	using HashGroupPtr = unique_ptr<ColumnDataCollection>;
	using OrderMasks = unordered_map<idx_t, ValidityMask>;
	using ExecutorGlobalStatePtr = unique_ptr<GlobalSinkState>;
	using ExecutorGlobalStates = vector<ExecutorGlobalStatePtr>;
	using ExecutorLocalStatePtr = unique_ptr<LocalSinkState>;
	using ExecutorLocalStates = vector<ExecutorLocalStatePtr>;
	using ThreadLocalStates = vector<ExecutorLocalStates>;
	using Task = WindowSourceTask;
	using TaskPtr = optional_ptr<Task>;
	using ScannerPtr = unique_ptr<WindowCollectionChunkScanner>;

	WindowHashGroup(WindowGlobalSinkState &gsink, HashGroupPtr &sorted, const idx_t hash_bin_p);

	void AllocateMasks();
	void ComputeMasks(const idx_t begin_idx, const idx_t end_idx);

	ExecutorGlobalStates &GetGlobalStates(ClientContext &client);

	// The total number of tasks we will execute per thread
	inline idx_t GetTaskCount() const {
		return GetThreadCount() * (uint8_t(WindowGroupStage::DONE) - uint8_t(WindowGroupStage::MASK));
	}
	// The total number of threads we will use
	inline idx_t GetThreadCount() const {
		return group_threads;
	}
	// Set up the task parameters
	idx_t InitTasks(idx_t per_thread);

	// Scan all of the chunks, starting at a given point
	ScannerPtr GetScanner(const idx_t begin_idx) const;
	void UpdateScanner(ScannerPtr &scanner, idx_t begin_idx) const;

	// The processing stage for this group
	WindowGroupStage GetStage() const {
		return stage;
	}

	bool TryPrepareNextStage() {
		lock_guard<mutex> prepare_guard(lock);
		switch (stage.load()) {
		case WindowGroupStage::MASK:
			if (masked == blocks) {
				stage = WindowGroupStage::SINK;
				return true;
			}
			return false;
		case WindowGroupStage::SINK:
			if (sunk == count) {
				stage = WindowGroupStage::FINALIZE;
				return true;
			}
			return false;
		case WindowGroupStage::FINALIZE:
			if (finalized == blocks) {
				stage = WindowGroupStage::GETDATA;
				return true;
			}
			return false;
		case WindowGroupStage::GETDATA:
		case WindowGroupStage::DONE:
			// never block in GETDATA
			return true;
		}

		//	Stop Linux whinging about control flow...
		return true;
	}

	bool TryNextTask(Task &task) {
		if (next_task >= GetTaskCount()) {
			return false;
		}
		const auto group_stage = GetStage();
		const auto group_threads = GetThreadCount();
		task.stage = WindowGroupStage(next_task / group_threads);
		if (task.stage == group_stage) {
			task.thread_idx = next_task % group_threads;
			task.group_idx = hash_bin;
			task.begin_idx = task.thread_idx * per_thread;
			task.max_idx = rows->ChunkCount();
			task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, task.max_idx);
			++next_task;
			return true;
		}

		return false;
	}

	//! The shared global state from sinking
	WindowGlobalSinkState &gsink;
	//! The hash partition data
	HashGroupPtr hash_group;
	//! The size of the group
	idx_t count = 0;
	//! The number of blocks in the group
	idx_t blocks = 0;
	unique_ptr<ColumnDataCollection> rows;
	TupleDataLayout layout;
	//! The partition boundary mask
	ValidityMask partition_mask;
	//! The order boundary mask
	OrderMasks order_masks;
	//! The fully materialised data collection
	unique_ptr<WindowCollection> collection;
	// The processing stage for this group
	atomic<WindowGroupStage> stage;
	//! The function global states for this hash group
	ExecutorGlobalStates gestates;
	//! Executor local states, one per thread
	ThreadLocalStates thread_states;

	//! The bin number
	idx_t hash_bin;
	//! Single threading lock
	mutex lock;
	//! The the number of blocks per thread.
	idx_t per_thread = 0;
	//! The the number of blocks per thread.
	idx_t group_threads = 0;
	//! The next task to process
	idx_t next_task = 0;
	//! Count of masked blocks
	std::atomic<idx_t> masked;
	//! Count of sunk rows
	std::atomic<idx_t> sunk;
	//! Count of finalized blocks
	std::atomic<idx_t> finalized;
	//! Count of completed tasks
	std::atomic<idx_t> completed;
	//! The output ordering batch index this hash group starts at
	idx_t batch_base;
};

class WindowGlobalSinkState : public GlobalSinkState {
public:
	using ExecutorPtr = unique_ptr<WindowExecutor>;
	using Executors = vector<ExecutorPtr>;

	WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &context);

	SinkFinalizeType Finalize(ClientContext &client, InterruptState &interrupt_state) {
		OperatorSinkFinalizeInput finalize {*hashed_sink, interrupt_state};
		auto result = global_partition->Finalize(client, finalize);

		return result;
	}

	//! Parent operator
	const PhysicalWindow &op;
	//! Client context
	ClientContext &client;
	//! The partitioned sunk data
	unique_ptr<HashedSort> global_partition;
	//! The partitioned sunk data
	unique_ptr<GlobalSinkState> hashed_sink;
	//! The number of sunk rows (for progress)
	atomic<idx_t> count;
	//! The execution functions
	Executors executors;
	//! The shared expressions library
	WindowSharedExpressions shared;
};

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	WindowLocalSinkState(ExecutionContext &context, const WindowGlobalSinkState &gstate)
	    : local_group(gstate.global_partition->GetLocalSinkState(context)) {
	}

	unique_ptr<LocalSinkState> local_group;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(PhysicalPlan &physical_plan, vector<LogicalType> types,
                               vector<unique_ptr<Expression>> select_list_p, idx_t estimated_cardinality,
                               PhysicalOperatorType type)
    : PhysicalOperator(physical_plan, type, std::move(types), estimated_cardinality),
      select_list(std::move(select_list_p)), order_idx(0), is_order_dependent(false) {

	idx_t max_orders = 0;
	for (idx_t i = 0; i < select_list.size(); ++i) {
		auto &expr = select_list[i];
		D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &bound_window = expr->Cast<BoundWindowExpression>();
		if (bound_window.partitions.empty() && bound_window.orders.empty()) {
			is_order_dependent = true;
		}

		if (bound_window.orders.size() > max_orders) {
			order_idx = i;
			max_orders = bound_window.orders.size();
		}
	}
}

static unique_ptr<WindowExecutor> WindowExecutorFactory(BoundWindowExpression &wexpr, ClientContext &client,
                                                        WindowSharedExpressions &shared, WindowAggregationMode mode) {
	switch (wexpr.GetExpressionType()) {
	case ExpressionType::WINDOW_AGGREGATE:
		return make_uniq<WindowAggregateExecutor>(wexpr, client, shared, mode);
	case ExpressionType::WINDOW_ROW_NUMBER:
		return make_uniq<WindowRowNumberExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_RANK_DENSE:
		return make_uniq<WindowDenseRankExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_RANK:
		return make_uniq<WindowRankExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_PERCENT_RANK:
		return make_uniq<WindowPercentRankExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_CUME_DIST:
		return make_uniq<WindowCumeDistExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_NTILE:
		return make_uniq<WindowNtileExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		return make_uniq<WindowLeadLagExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_FILL:
		return make_uniq<WindowFillExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_FIRST_VALUE:
		return make_uniq<WindowFirstValueExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_LAST_VALUE:
		return make_uniq<WindowLastValueExecutor>(wexpr, shared);
	case ExpressionType::WINDOW_NTH_VALUE:
		return make_uniq<WindowNthValueExecutor>(wexpr, shared);
		break;
	default:
		throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr.GetExpressionType()));
	}
}

WindowGlobalSinkState::WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &client)
    : op(op), client(client), count(0) {

	D_ASSERT(op.select_list[op.order_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
	auto &wexpr = op.select_list[op.order_idx]->Cast<BoundWindowExpression>();

	const auto mode = DBConfig::GetSetting<DebugWindowModeSetting>(client);
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
		auto wexec = WindowExecutorFactory(wexpr, client, shared, mode);
		executors.emplace_back(std::move(wexec));
	}

	global_partition = make_uniq<HashedSort>(client, wexpr.partitions, wexpr.orders, op.children[0].get().GetTypes(),
	                                         wexpr.partitions_stats, op.estimated_cardinality);
	hashed_sink = global_partition->GetGlobalSinkState(client);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalWindow::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &sink) const {
	auto &gstate = sink.global_state.Cast<WindowGlobalSinkState>();
	auto &lstate = sink.local_state.Cast<WindowLocalSinkState>();
	gstate.count += chunk.size();

	OperatorSinkInput hsink {*gstate.hashed_sink, *lstate.local_group, sink.interrupt_state};
	return gstate.global_partition->Sink(context, chunk, hsink);
}

SinkCombineResultType PhysicalWindow::Combine(ExecutionContext &context, OperatorSinkCombineInput &combine) const {
	auto &gstate = combine.global_state.Cast<WindowGlobalSinkState>();
	auto &lstate = combine.local_state.Cast<WindowLocalSinkState>();

	OperatorSinkCombineInput hcombine {*gstate.hashed_sink, *lstate.local_group, combine.interrupt_state};
	return gstate.global_partition->Combine(context, hcombine);
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowLocalSinkState>(context, gstate);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &client) const {
	return make_uniq<WindowGlobalSinkState>(*this, client);
}

SinkFinalizeType PhysicalWindow::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gsink = input.global_state.Cast<WindowGlobalSinkState>();
	auto &global_partition = *gsink.global_partition;
	auto &hashed_sink = *gsink.hashed_sink;

	OperatorSinkFinalizeInput hfinalize {hashed_sink, input.interrupt_state};
	auto result = global_partition.Finalize(client, hfinalize);

	//	Did we get any data?
	if (result != SinkFinalizeType::READY) {
		return result;
	}

	return global_partition.MaterializeHashGroups(pipeline, event, *this, hfinalize);
}

ProgressData PhysicalWindow::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                             const ProgressData source_progress) const {
	auto &gsink = gstate.Cast<WindowGlobalSinkState>();
	return gsink.global_partition->GetSinkProgress(context, *gsink.hashed_sink, source_progress);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	using WindowHashGroupPtr = unique_ptr<WindowHashGroup>;
	using ScannerPtr = unique_ptr<TupleDataChunkIterator>;
	using Task = WindowSourceTask;
	using TaskPtr = optional_ptr<Task>;
	using PartitionBlock = std::pair<idx_t, idx_t>;

	WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p);

	//! Are there any more tasks?
	bool HasMoreTasks() const {
		return !stopped && started < total_tasks;
	}
	bool HasUnfinishedTasks() const {
		return !stopped && finished < total_tasks;
	}
	//! Get the next task given the current state
	bool TryNextTask(TaskPtr &task, Task &task_local);

	//! Context for executing computations
	ClientContext &client;
	//! All the sunk data
	WindowGlobalSinkState &gsink;
	//! The sorted hash groups
	vector<WindowHashGroupPtr> window_hash_groups;
	//! The total number of blocks to process;
	idx_t total_blocks = 0;
	//! The sorted list of (blocks, group_idx) pairs
	vector<PartitionBlock> partition_blocks;
	//! The ordered set of active groups
	vector<idx_t> active_groups;
	//! The number of started tasks
	atomic<idx_t> next_group;
	//! The number of local states
	atomic<idx_t> locals;
	//! The total number of tasks
	idx_t total_tasks = 0;
	//! The number of started tasks
	atomic<idx_t> started;
	//! The number of finished tasks
	atomic<idx_t> finished;
	//! Stop producing tasks
	atomic<bool> stopped;
	//! The number of tasks completed. This will combine both build and evaluate.
	atomic<idx_t> completed;

public:
	idx_t MaxThreads() override {
		return total_blocks;
	}

protected:
	//! Build task list
	void CreateTaskList();
	//! Finish a task
	void FinishTask(TaskPtr task);
};

WindowGlobalSourceState::WindowGlobalSourceState(ClientContext &client, WindowGlobalSinkState &gsink_p)
    : client(client), gsink(gsink_p), next_group(0), locals(0), started(0), finished(0), stopped(false), completed(0) {

	auto &global_partition = *gsink.global_partition;
	auto hashed_source = global_partition.GetGlobalSourceState(client, *gsink.hashed_sink);
	auto &hash_groups = global_partition.GetHashGroups(*hashed_source);
	window_hash_groups.resize(hash_groups.size());

	for (idx_t group_idx = 0; group_idx < hash_groups.size(); ++group_idx) {
		auto rows = std::move(hash_groups[group_idx]);
		if (!rows) {
			continue;
		}

		auto window_hash_group = make_uniq<WindowHashGroup>(gsink, rows, group_idx);
		const auto block_count = window_hash_group->rows->ChunkCount();
		window_hash_group->batch_base = total_blocks;
		total_blocks += block_count;

		window_hash_groups[group_idx] = std::move(window_hash_group);
	}

	CreateTaskList();
}

void WindowGlobalSourceState::CreateTaskList() {
	//    Sort the groups from largest to smallest
	if (window_hash_groups.empty()) {
		return;
	}

	for (idx_t group_idx = 0; group_idx < window_hash_groups.size(); ++group_idx) {
		auto &window_hash_group = window_hash_groups[group_idx];
		if (!window_hash_group) {
			continue;
		}
		partition_blocks.emplace_back(window_hash_group->rows->ChunkCount(), group_idx);
	}
	std::sort(partition_blocks.begin(), partition_blocks.end(), std::greater<PartitionBlock>());

	//	Schedule the largest group on as many threads as possible
	auto &ts = TaskScheduler::GetScheduler(client);
	const auto threads = NumericCast<idx_t>(ts.NumberOfThreads());

	const auto &max_block = partition_blocks.front();

	// To compute masks in parallel, we need to have the row count of the number of chunks per thread
	// be a multiple of the mask entry size.  Usually, this is not a problem because
	// STANDARD_VECTOR_SIZE >> ValidityMask::BITS_PER_VALUE, but if STANDARD_VECTOR_SIZE is say 2,
	// we need to align the chunk count to the mask width.
	const auto aligned_scale = MaxValue<idx_t>(ValidityMask::BITS_PER_VALUE / STANDARD_VECTOR_SIZE, 1);
	const auto aligned_count = (max_block.first + aligned_scale - 1) / aligned_scale;
	const auto per_thread = aligned_scale * ((aligned_count + threads - 1) / threads);
	if (!per_thread) {
		throw InternalException("No blocks per thread! %ld threads, %ld groups, %ld blocks, %ld hash group", threads,
		                        partition_blocks.size(), max_block.first, max_block.second);
	}

	for (const auto &b : partition_blocks) {
		total_tasks += window_hash_groups[b.second]->InitTasks(per_thread);
	}
}

WindowHashGroup::WindowHashGroup(WindowGlobalSinkState &gsink, HashGroupPtr &sorted, const idx_t hash_bin_p)
    : gsink(gsink), count(0), blocks(0), rows(std::move(sorted)), stage(WindowGroupStage::MASK), hash_bin(hash_bin_p),
      masked(0), sunk(0), finalized(0), completed(0), batch_base(0) {
	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	auto &gpart = *gsink.global_partition;
	layout.Initialize(gpart.payload_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);

	if (rows) {
		count = rows->Count();
		blocks = rows->ChunkCount();
	}

	// Set up the collection for any fully materialised data
	const auto &shared = WindowSharedExpressions::GetSortedExpressions(gsink.shared.coll_shared);
	vector<LogicalType> types;
	for (auto &expr : shared) {
		types.emplace_back(expr->return_type);
	}
	auto &buffer_manager = BufferManager::GetBufferManager(gsink.client);
	collection = make_uniq<WindowCollection>(buffer_manager, count, types);
}

unique_ptr<WindowCollectionChunkScanner> WindowHashGroup::GetScanner(const idx_t begin_idx) const {
	if (!rows) {
		return nullptr;
	}

	auto &scan_ids = gsink.global_partition->scan_ids;
	return make_uniq<WindowCollectionChunkScanner>(*rows, scan_ids, begin_idx);
}

void WindowHashGroup::UpdateScanner(ScannerPtr &scanner, idx_t begin_idx) const {
	if (!scanner || &scanner->collection != rows.get()) {
		scanner.reset();
		scanner = GetScanner(begin_idx);
	} else {
		scanner->Seek(begin_idx);
	}
}

void WindowHashGroup::AllocateMasks() {
	//	Single-threaded building as this is mostly memory allocation
	lock_guard<mutex> gestate_guard(lock);
	if (partition_mask.IsMaskSet()) {
		return;
	}

	//	Allocate masks inside the lock
	partition_mask.Initialize(count);

	const auto &executors = gsink.executors;
	for (auto &wexec : executors) {
		auto &wexpr = wexec->wexpr;
		auto &order_mask = order_masks[wexpr.partitions.size() + wexpr.orders.size()];
		if (order_mask.IsMaskSet()) {
			continue;
		}
		order_mask.Initialize(count);
	}
}

void WindowHashGroup::ComputeMasks(const idx_t block_begin, const idx_t block_end) {
	D_ASSERT(count > 0);

	//	Initialise our range
	AllocateMasks();
	const auto begin_entry = partition_mask.EntryCount(block_begin * STANDARD_VECTOR_SIZE);
	const auto end_entry = partition_mask.EntryCount(MinValue<idx_t>(block_end * STANDARD_VECTOR_SIZE, count));

	//	If the data is unsorted, then the chunk sizes may be < STANDARD_VECTOR_SIZE,
	//	and the entry range may be empty.
	if (begin_entry >= end_entry) {
		D_ASSERT(gsink.global_partition->sort_col_count == 0);
		return;
	}

	partition_mask.SetRangeInvalid(count, begin_entry, end_entry);
	if (!block_begin) {
		partition_mask.SetValidUnsafe(0);
	}
	for (auto &order_mask : order_masks) {
		order_mask.second.SetRangeInvalid(count, begin_entry, end_entry);
		if (!block_begin) {
			order_mask.second.SetValidUnsafe(0);
		}
	}

	//	If we are not sorting, then only the partition boundaries are needed.
	if (!gsink.global_partition->sort) {
		return;
	}

	//	Set up the partition compare structs
	auto &partitions = gsink.global_partition->partitions;
	const auto key_count = partitions.size();

	//	Set up the order data structures
	auto &collection = *rows;
	auto &scan_cols = gsink.global_partition->sort_ids;
	WindowCollectionChunkScanner scanner(collection, scan_cols, block_begin);
	unordered_map<idx_t, DataChunk> prefixes;
	for (auto &order_mask : order_masks) {
		D_ASSERT(order_mask.first >= partitions.size());
		auto order_type = scanner.PrefixStructType(order_mask.first, partitions.size());
		vector<LogicalType> types(2, order_type);
		auto &keys = prefixes[order_mask.first];
		// We can't use InitializeEmpty here because it doesn't set up all of the STRUCT internals...
		keys.Initialize(collection.GetAllocator(), types);
	}

	WindowDeltaScanner(collection, block_begin, block_end, scan_cols, key_count,
	                   [&](const idx_t row_idx, DataChunk &prev, DataChunk &curr, const idx_t ndistinct,
	                       SelectionVector &distinct, const SelectionVector &matching) {
		                   //	Process the partition boundaries
		                   for (idx_t i = 0; i < ndistinct; ++i) {
			                   const idx_t curr_index = row_idx + distinct.get_index(i);
			                   partition_mask.SetValidUnsafe(curr_index);
			                   for (auto &order_mask : order_masks) {
				                   order_mask.second.SetValidUnsafe(curr_index);
			                   }
		                   }

		                   //	Process the peers with each partition
		                   const auto count = MinValue<idx_t>(prev.size(), curr.size());
		                   const auto nmatch = count - ndistinct;
		                   if (!nmatch) {
			                   return;
		                   }

		                   for (auto &order_mask : order_masks) {
			                   // If there are no order columns, then all the partition elements are peers and we are
			                   // done
			                   if (partitions.size() == order_mask.first) {
				                   continue;
			                   }
			                   auto &prefix = prefixes[order_mask.first];
			                   prefix.Reset();
			                   auto &order_prev = prefix.data[0];
			                   auto &order_curr = prefix.data[1];
			                   scanner.ReferenceStructColumns(prev, order_prev, order_mask.first, partitions.size());
			                   scanner.ReferenceStructColumns(curr, order_curr, order_mask.first, partitions.size());
			                   if (ndistinct) {
				                   prefix.Slice(matching, nmatch);
			                   } else {
				                   prefix.SetCardinality(nmatch);
			                   }
			                   const auto m = VectorOperations::DistinctFrom(order_curr, order_prev, nullptr, nmatch,
			                                                                 &distinct, nullptr);
			                   for (idx_t i = 0; i < m; ++i) {
				                   const idx_t curr_index = row_idx + matching.get_index(distinct.get_index(i));
				                   order_mask.second.SetValidUnsafe(curr_index);
			                   }
		                   }
	                   });
}

// Per-thread scan state
class WindowLocalSourceState : public LocalSourceState {
public:
	using Task = WindowGlobalSourceState::Task;
	using TaskPtr = optional_ptr<Task>;

	explicit WindowLocalSourceState(WindowGlobalSourceState &gsource);

	void ReleaseLocalStates() {
		auto &local_states = window_hash_group->thread_states.at(task->thread_idx);
		local_states.clear();
	}

	//! Does the task have more work to do?
	bool TaskFinished() const {
		return !task || task->begin_idx == task->end_idx;
	}
	//! Assign the next task
	bool TryAssignTask();
	//! Execute a step in the current task
	void ExecuteTask(ExecutionContext &context, DataChunk &chunk, InterruptState &interrupt);

	//! The shared source state
	WindowGlobalSourceState &gsource;
	//! The current batch index (for output reordering)
	idx_t batch_index;
	//! The task this thread is working on
	TaskPtr task;
	//! The task storage
	Task task_local;
	//! The current source being processed
	optional_ptr<WindowHashGroup> window_hash_group;
	//! The scan cursor
	unique_ptr<WindowCollectionChunkScanner> scanner;
	//! Buffer for window results
	DataChunk output_chunk;

protected:
	//! Compute a mask range
	void Mask(ExecutionContext &context, InterruptState &interrupt);
	//! Sink tuples into function global states
	void Sink(ExecutionContext &context, InterruptState &interrupt);
	//! Post process function global state construction
	void Finalize(ExecutionContext &context, InterruptState &interrupt);
	//! Get a chunk by evaluating functions
	void GetData(ExecutionContext &context, DataChunk &chunk, InterruptState &interrupt);

	//! Storage and evaluation for the fully materialised data
	unique_ptr<WindowBuilder> builder;
	ExpressionExecutor coll_exec;
	DataChunk coll_chunk;

	//! Storage and evaluation for chunks used in the sink/build phase
	ExpressionExecutor sink_exec;
	DataChunk sink_chunk;

	//! Storage and evaluation for chunks used in the evaluate phase
	ExpressionExecutor eval_exec;
	DataChunk eval_chunk;
};

idx_t WindowHashGroup::InitTasks(idx_t per_thread_p) {
	per_thread = per_thread_p;
	group_threads = (rows->ChunkCount() + per_thread - 1) / per_thread;
	thread_states.resize(GetThreadCount());

	return GetTaskCount();
}

void WindowLocalSourceState::Mask(ExecutionContext &context, InterruptState &interrupt) {
	D_ASSERT(task);
	D_ASSERT(task->stage == WindowGroupStage::MASK);

	window_hash_group->ComputeMasks(task->begin_idx, task->end_idx);

	//	Mark this range as done
	window_hash_group->masked += (task->end_idx - task->begin_idx);
	task->begin_idx = task->end_idx;
}

WindowHashGroup::ExecutorGlobalStates &WindowHashGroup::GetGlobalStates(ClientContext &client) {
	//	Single-threaded building as this is mostly memory allocation
	lock_guard<mutex> gestate_guard(lock);
	const auto &executors = gsink.executors;
	if (gestates.size() == executors.size()) {
		return gestates;
	}

	// These can be large so we defer building them until we are ready.
	for (auto &wexec : executors) {
		auto &wexpr = wexec->wexpr;
		auto &order_mask = order_masks[wexpr.partitions.size() + wexpr.orders.size()];
		gestates.emplace_back(wexec->GetGlobalState(client, count, partition_mask, order_mask));
	}

	return gestates;
}

void WindowLocalSourceState::Sink(ExecutionContext &context, InterruptState &interrupt) {
	D_ASSERT(task);
	D_ASSERT(task->stage == WindowGroupStage::SINK);

	auto &gsink = gsource.gsink;
	const auto &executors = gsink.executors;

	// Create the global state for each function
	// These can be large so we defer building them until we are ready.
	auto &gestates = window_hash_group->GetGlobalStates(context.client);

	//	Set up the local states
	auto &local_states = window_hash_group->thread_states.at(task->thread_idx);
	if (local_states.empty()) {
		for (idx_t w = 0; w < executors.size(); ++w) {
			local_states.emplace_back(executors[w]->GetLocalState(context, *gestates[w]));
		}
	}

	//	First pass over the input without flushing
	scanner = window_hash_group->GetScanner(task->begin_idx);
	if (!scanner) {
		return;
	}
	for (; task->begin_idx < task->end_idx; ++task->begin_idx) {
		const idx_t input_idx = scanner->Scanned();
		if (!scanner->Scan()) {
			break;
		}
		auto &input_chunk = scanner->chunk;

		//	Compute fully materialised expressions
		if (coll_chunk.data.empty()) {
			coll_chunk.SetCardinality(input_chunk);
		} else {
			coll_chunk.Reset();
			coll_exec.Execute(input_chunk, coll_chunk);
			auto collection = window_hash_group->collection.get();
			if (!builder || &builder->collection != collection) {
				builder = make_uniq<WindowBuilder>(*collection);
			}

			builder->Sink(coll_chunk, input_idx);
		}

		// Compute sink expressions
		if (sink_chunk.data.empty()) {
			sink_chunk.SetCardinality(input_chunk);
		} else {
			sink_chunk.Reset();
			sink_exec.Execute(input_chunk, sink_chunk);
		}

		for (idx_t w = 0; w < executors.size(); ++w) {
			OperatorSinkInput sink {*gestates[w], *local_states[w], interrupt};
			executors[w]->Sink(context, sink_chunk, coll_chunk, input_idx, sink);
		}

		window_hash_group->sunk += input_chunk.size();
	}
	scanner.reset();
}

void WindowLocalSourceState::Finalize(ExecutionContext &context, InterruptState &interrupt) {
	D_ASSERT(task);
	D_ASSERT(task->stage == WindowGroupStage::FINALIZE);

	// First finalize the collection (so the executors can use it)
	auto &gsink = gsource.gsink;
	if (window_hash_group->collection) {
		window_hash_group->collection->Combine(gsink.shared.coll_validity);
	}

	// Finalize all the executors.
	// Parallel finalisation is handled internally by the executor,
	// and should not return until all threads have completed work.
	const auto &executors = gsink.executors;
	auto &gestates = window_hash_group->gestates;
	auto &local_states = window_hash_group->thread_states.at(task->thread_idx);
	for (idx_t w = 0; w < executors.size(); ++w) {
		OperatorSinkInput sink {*gestates[w], *local_states[w], interrupt};
		executors[w]->Finalize(context, window_hash_group->collection, sink);
	}

	//	Mark this range as done
	window_hash_group->finalized += (task->end_idx - task->begin_idx);
	task->begin_idx = task->end_idx;
}

WindowLocalSourceState::WindowLocalSourceState(WindowGlobalSourceState &gsource)
    : gsource(gsource), batch_index(0), coll_exec(gsource.client), sink_exec(gsource.client),
      eval_exec(gsource.client) {
	auto &gsink = gsource.gsink;

	vector<LogicalType> output_types;
	for (auto &wexec : gsink.executors) {
		auto &wexpr = wexec->wexpr;
		output_types.emplace_back(wexpr.return_type);
	}
	output_chunk.Initialize(gsource.client, output_types);

	auto &shared = gsink.shared;
	shared.PrepareCollection(coll_exec, coll_chunk);
	shared.PrepareSink(sink_exec, sink_chunk);
	shared.PrepareEvaluate(eval_exec, eval_chunk);

	++gsource.locals;
}

bool WindowGlobalSourceState::TryNextTask(TaskPtr &task, Task &task_local) {
	auto guard = Lock();
	FinishTask(task);

	if (!HasMoreTasks()) {
		task = nullptr;
		return false;
	}

	//	Run through the active groups looking for one that can assign a task
	for (const auto &group_idx : active_groups) {
		auto &window_hash_group = window_hash_groups[group_idx];
		if (window_hash_group->TryPrepareNextStage()) {
			UnblockTasks(guard);
		}
		if (window_hash_group->TryNextTask(task_local)) {
			task = task_local;
			++started;
			return true;
		}
	}

	//	All active groups are busy or blocked, so start the next one (if any)
	while (next_group < partition_blocks.size()) {
		const auto group_idx = partition_blocks[next_group++].second;
		active_groups.emplace_back(group_idx);

		auto &window_hash_group = window_hash_groups[group_idx];
		if (window_hash_group->TryPrepareNextStage()) {
			UnblockTasks(guard);
		}
		if (!window_hash_group->TryNextTask(task_local)) {
			//	Group has no tasks (empty?)
			continue;
		}

		task = task_local;
		++started;
		return true;
	}

	task = nullptr;

	return false;
}

void WindowGlobalSourceState::FinishTask(TaskPtr task) {
	if (!task) {
		return;
	}

	const auto group_idx = task->group_idx;
	auto &finished_hash_group = window_hash_groups[group_idx];
	D_ASSERT(finished_hash_group);

	if (++finished_hash_group->completed >= finished_hash_group->GetTaskCount()) {
		finished_hash_group.reset();
		//	Remove it from the active groups
		auto &v = active_groups;
		v.erase(std::remove(v.begin(), v.end(), group_idx), v.end());
	}

	//	Count the global tasks completed.
	++completed;
}

bool WindowLocalSourceState::TryAssignTask() {
	D_ASSERT(TaskFinished());
	if (task && task->stage == WindowGroupStage::GETDATA) {
		// If this state completed the last block in the previous iteration,
		// release our local state memory.
		ReleaseLocalStates();
	}
	// Because downstream operators may be using our internal buffers,
	// we can't "finish" a task until we are about to get the next one.

	// Scanner first, as it may be referencing sort blocks in the hash group
	scanner.reset();

	return gsource.TryNextTask(task, task_local);
}

void WindowLocalSourceState::ExecuteTask(ExecutionContext &context, DataChunk &result, InterruptState &interrupt) {
	// Update the hash group
	window_hash_group = gsource.window_hash_groups[task->group_idx].get();

	// Process the new state
	switch (task->stage) {
	case WindowGroupStage::MASK:
		Mask(context, interrupt);
		D_ASSERT(TaskFinished());
		break;
	case WindowGroupStage::SINK:
		Sink(context, interrupt);
		D_ASSERT(TaskFinished());
		break;
	case WindowGroupStage::FINALIZE:
		Finalize(context, interrupt);
		D_ASSERT(TaskFinished());
		break;
	case WindowGroupStage::GETDATA:
		D_ASSERT(!TaskFinished());
		GetData(context, result, interrupt);
		break;
	case WindowGroupStage::DONE:
		throw InternalException("Invalid window source state.");
	}

	// Count this task as finished.
	if (TaskFinished()) {
		++gsource.finished;
	}
}

void WindowLocalSourceState::GetData(ExecutionContext &context, DataChunk &result, InterruptState &interrupt) {
	D_ASSERT(window_hash_group->GetStage() == WindowGroupStage::GETDATA);

	window_hash_group->UpdateScanner(scanner, task->begin_idx);
	batch_index = window_hash_group->batch_base + task->begin_idx;

	const auto position = scanner->Scanned();
	auto &input_chunk = scanner->chunk;
	scanner->Scan();

	const auto &executors = gsource.gsink.executors;
	auto &gestates = window_hash_group->gestates;
	auto &local_states = window_hash_group->thread_states.at(task->thread_idx);
	output_chunk.Reset();
	for (idx_t expr_idx = 0; expr_idx < executors.size(); ++expr_idx) {
		auto &executor = *executors[expr_idx];
		auto &result = output_chunk.data[expr_idx];
		if (eval_chunk.data.empty()) {
			eval_chunk.SetCardinality(input_chunk);
		} else {
			eval_chunk.Reset();
			eval_exec.Execute(input_chunk, eval_chunk);
		}
		OperatorSinkInput sink {*gestates[expr_idx], *local_states[expr_idx], interrupt};
		executor.Evaluate(context, position, eval_chunk, result, sink);
	}
	output_chunk.SetCardinality(input_chunk);
	output_chunk.Verify();

	idx_t out_idx = 0;
	result.SetCardinality(input_chunk);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(input_chunk.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < output_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(output_chunk.data[col_idx]);
	}

	// Move to the next chunk
	++task->begin_idx;

	result.Verify();
}

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gsource_p) const {
	auto &gsource = gsource_p.Cast<WindowGlobalSourceState>();
	return make_uniq<WindowLocalSourceState>(gsource);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &client) const {
	auto &gsink = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowGlobalSourceState>(client, gsink);
}

bool PhysicalWindow::SupportsPartitioning(const OperatorPartitionInfo &partition_info) const {
	if (partition_info.RequiresPartitionColumns()) {
		return false;
	}
	//	We can only preserve order for single partitioning
	//	or work stealing causes out of order batch numbers
	auto &wexpr = select_list[order_idx]->Cast<BoundWindowExpression>();
	return wexpr.partitions.empty(); // NOLINT
}

OrderPreservationType PhysicalWindow::SourceOrder() const {
	auto &wexpr = select_list[order_idx]->Cast<BoundWindowExpression>();
	if (!wexpr.partitions.empty()) {
		// if we have partitions the window order is not defined
		return OrderPreservationType::NO_ORDER;
	}
	// without partitions we can maintain order
	if (wexpr.orders.empty()) {
		// if we have no orders we maintain insertion order
		return OrderPreservationType::INSERTION_ORDER;
	}
	// otherwise we can maintain the fixed order
	return OrderPreservationType::FIXED_ORDER;
}

ProgressData PhysicalWindow::GetProgress(ClientContext &client, GlobalSourceState &gsource_p) const {
	auto &gsource = gsource_p.Cast<WindowGlobalSourceState>();
	auto &gsink = gsource.gsink;
	const auto count = gsink.count.load();
	const auto completed = gsource.completed.load();

	ProgressData res;
	if (count) {
		res.done = double(completed);
		res.total = double(gsource.total_tasks);
		//	Convert to tuples.
		res.Normalize(double(count));
	} else {
		res.SetInvalid();
	}

	return res;
}

OperatorPartitionData PhysicalWindow::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                       GlobalSourceState &gstate_p, LocalSourceState &lstate_p,
                                                       const OperatorPartitionInfo &partition_info) const {
	if (partition_info.RequiresPartitionColumns()) {
		throw InternalException("PhysicalWindow::GetPartitionData: partition columns not supported");
	}
	auto &lstate = lstate_p.Cast<WindowLocalSourceState>();
	return OperatorPartitionData(lstate.batch_index);
}

SourceResultType PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &source) const {
	auto &gsource = source.global_state.Cast<WindowGlobalSourceState>();
	auto &lsource = source.local_state.Cast<WindowLocalSourceState>();

	while (gsource.HasUnfinishedTasks() && chunk.size() == 0) {
		if (!lsource.TaskFinished() || lsource.TryAssignTask()) {
			try {
				lsource.ExecuteTask(context, chunk, source.interrupt_state);
			} catch (...) {
				gsource.stopped = true;
				throw;
			}
		} else {
			auto guard = gsource.Lock();
			if (!gsource.HasMoreTasks()) {
				// no more tasks - exit
				gsource.UnblockTasks(guard);
				break;
			} else {
				// there are more tasks available, but we can't execute them yet
				// block the source
				return gsource.BlockSource(guard, source.interrupt_state);
			}
		}
	}

	if (chunk.size() == 0) {
		return SourceResultType::FINISHED;
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

InsertionOrderPreservingMap<string> PhysicalWindow::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string projections;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			projections += "\n";
		}
		projections += select_list[i]->GetName();
	}
	result["Projections"] = projections;
	return result;
}

} // namespace duckdb
