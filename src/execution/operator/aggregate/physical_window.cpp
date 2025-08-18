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

enum WindowGroupStage : uint8_t { SINK, FINALIZE, GETDATA, DONE };

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
	using HashGroupPtr = unique_ptr<HashedSortGroup>;
	using OrderMasks = HashedSortGroup::OrderMasks;
	using ExecutorGlobalStatePtr = unique_ptr<WindowExecutorGlobalState>;
	using ExecutorGlobalStates = vector<ExecutorGlobalStatePtr>;
	using ExecutorLocalStatePtr = unique_ptr<WindowExecutorLocalState>;
	using ExecutorLocalStates = vector<ExecutorLocalStatePtr>;
	using ThreadLocalStates = vector<ExecutorLocalStates>;
	using Task = WindowSourceTask;
	using TaskPtr = optional_ptr<Task>;
	using ScannerPtr = unique_ptr<WindowCollectionChunkScanner>;

	WindowHashGroup(WindowGlobalSinkState &gsink, const idx_t hash_bin_p);

	void ComputeMasks(ValidityMask &partition_mask, OrderMasks &order_masks);

	ExecutorGlobalStates &Initialize(ClientContext &client);

	// The total number of tasks we will execute (SINK, FINALIZE, GETDATA per thread)
	inline idx_t GetTaskCount() const {
		return GetThreadCount() * 3;
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
		default:
			// never block in GETDATA
			return true;
		}
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
	using WindowHashGroupPtr = unique_ptr<WindowHashGroup>;
	using ExecutorPtr = unique_ptr<WindowExecutor>;
	using Executors = vector<ExecutorPtr>;

	class Callback : public HashedSortCallback {
	public:
		explicit Callback(GlobalSinkState &gsink) : gsink(gsink) {
		}

		void OnSortedGroup(HashedSortGroup &hash_group) override {
			gsink.Cast<WindowGlobalSinkState>().OnSortedGroup(hash_group);
		}

		GlobalSinkState &gsink;
	};

	WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &context);

	void Finalize(ClientContext &context, InterruptState &interrupt_state) {
		global_partition->Finalize(context, interrupt_state);
		window_hash_groups.resize(global_partition->hash_groups.size());
	}

	void OnSortedGroup(HashedSortGroup &hash_group) {
		window_hash_groups[hash_group.group_idx] = make_uniq<WindowHashGroup>(*this, hash_group.group_idx);
	}

	//! Parent operator
	const PhysicalWindow &op;
	//! Client context
	ClientContext &client;
	//! The partitioned sunk data
	unique_ptr<HashedSortGlobalSinkState> global_partition;
	//! The callback for completed hash groups
	Callback callback;
	//! The sorted hash groups
	vector<WindowHashGroupPtr> window_hash_groups;
	//! The execution functions
	Executors executors;
	//! The shared expressions library
	WindowSharedExpressions shared;
};

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	WindowLocalSinkState(ExecutionContext &context, const WindowGlobalSinkState &gstate)
	    : local_group(context, *gstate.global_partition) {
	}

	void Sink(ExecutionContext &context, DataChunk &input_chunk) {
		local_group.Sink(context, input_chunk);
	}

	void Combine(ExecutionContext &context) {
		local_group.Combine(context);
	}

	HashedSortLocalSinkState local_group;
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
    : op(op), client(client), callback(*this) {

	D_ASSERT(op.select_list[op.order_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
	auto &wexpr = op.select_list[op.order_idx]->Cast<BoundWindowExpression>();

	const auto mode = DBConfig::GetSetting<DebugWindowModeSetting>(client);
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
		auto wexec = WindowExecutorFactory(wexpr, client, shared, mode);
		executors.emplace_back(std::move(wexec));
	}

	global_partition =
	    make_uniq<HashedSortGlobalSinkState>(client, wexpr.partitions, wexpr.orders, op.children[0].get().GetTypes(),
	                                         wexpr.partitions_stats, op.estimated_cardinality);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalWindow::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();

	lstate.Sink(context, chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalWindow::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();
	lstate.Combine(context);

	return SinkCombineResultType::FINISHED;
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
	auto &gpart = *gsink.global_partition;

	//	Did we get any data?
	if (!gpart.count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// OVER()
	if (gpart.unsorted) {
		// We need to construct the single WindowHashGroup here because the sort tasks will not be run.
		D_ASSERT(!gpart.grouping_data);
		if (!gpart.unsorted->Count()) {
			return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
		}

		gsink.window_hash_groups.emplace_back(make_uniq<WindowHashGroup>(gsink, idx_t(0)));
		return SinkFinalizeType::READY;
	}

	gsink.Finalize(client, input.interrupt_state);

	// Find the first group to sort
	if (!gsink.global_partition->HasMergeTasks()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Schedule all the sorts for maximum thread utilisation
	auto sort_event = make_shared_ptr<HashedSortMaterializeEvent>(gpart, pipeline, *this, &gsink.callback);
	event.InsertEvent(std::move(sort_event));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
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
	//! The number of rows returned
	atomic<idx_t> returned;

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
    : client(client), gsink(gsink_p), next_group(0), locals(0), started(0), finished(0), stopped(false), returned(0) {
	auto &window_hash_groups = gsink.window_hash_groups;

	for (auto &window_hash_group : window_hash_groups) {
		if (!window_hash_group) {
			continue;
		}
		auto &rows = window_hash_group->rows;
		if (!rows) {
			continue;
		}

		const auto block_count = window_hash_group->rows->ChunkCount();
		window_hash_group->batch_base = total_blocks;
		total_blocks += block_count;
	}

	CreateTaskList();
}

void WindowGlobalSourceState::CreateTaskList() {
	//    Sort the groups from largest to smallest
	auto &window_hash_groups = gsink.window_hash_groups;
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
	const auto per_thread = (max_block.first + threads - 1) / threads;
	if (!per_thread) {
		throw InternalException("No blocks per thread! %ld threads, %ld groups, %ld blocks, %ld hash group", threads,
		                        partition_blocks.size(), max_block.first, max_block.second);
	}

	for (const auto &b : partition_blocks) {
		total_tasks += window_hash_groups[b.second]->InitTasks(per_thread);
	}
}

WindowHashGroup::WindowHashGroup(WindowGlobalSinkState &gsink, const idx_t hash_bin_p)
    : gsink(gsink), count(0), blocks(0), stage(WindowGroupStage::SINK), hash_bin(hash_bin_p), sunk(0), finalized(0),
      completed(0), batch_base(0) {
	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	auto &gpart = *gsink.global_partition;
	layout.Initialize(gpart.payload_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	if (hash_bin < gpart.hash_groups.size() && gpart.hash_groups[hash_bin]) {
		count = gpart.hash_groups[hash_bin]->sorted->Count();
	} else if (gpart.unsorted && !hash_bin) {
		count = gpart.count;
	} else {
		return;
	}

	//	Initialise masks to false
	partition_mask.Initialize(count);
	partition_mask.SetAllInvalid(count);

	const auto &executors = gsink.executors;
	for (auto &wexec : executors) {
		auto &wexpr = wexec->wexpr;
		auto &order_mask = order_masks[wexpr.partitions.size() + wexpr.orders.size()];
		if (order_mask.IsMaskSet()) {
			continue;
		}
		order_mask.Initialize(count);
		order_mask.SetAllInvalid(count);
	}

	// Scan the sorted data into new Collections
	if (gpart.unsorted && !hash_bin) {
		// Simple mask
		partition_mask.SetValidUnsafe(0);
		for (auto &order_mask : order_masks) {
			order_mask.second.SetValidUnsafe(0);
		}
		//	No partition - take ownership of the accumulated data
		rows = std::move(gpart.unsorted);
	} else if (hash_bin < gpart.hash_groups.size()) {
		// Overwrite the collections with the sorted data
		D_ASSERT(gpart.hash_groups[hash_bin].get());
		hash_group = std::move(gpart.hash_groups[hash_bin]);
		rows = std::move(hash_group->sorted);
		ComputeMasks(partition_mask, order_masks);
	}

	if (rows) {
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

void WindowHashGroup::ComputeMasks(ValidityMask &partition_mask, OrderMasks &order_masks) {
	D_ASSERT(count > 0);

	//	Set up the partition compare structs
	auto &partitions = gsink.global_partition->partitions;
	partition_mask.SetValidUnsafe(0);
	const auto key_count = partitions.size();

	//	Set up the order data structures
	auto &collection = *rows;
	auto &scan_cols = gsink.global_partition->sort_ids;
	WindowCollectionChunkScanner scanner(collection, scan_cols, 0);
	unordered_map<idx_t, DataChunk> prefixes;
	for (auto &order_mask : order_masks) {
		order_mask.second.SetValidUnsafe(0);
		D_ASSERT(order_mask.first >= partitions.size());
		auto order_type = scanner.PrefixStructType(order_mask.first, partitions.size());
		vector<LogicalType> types(2, order_type);
		auto &keys = prefixes[order_mask.first];
		// We can't use InitializeEmpty here because it doesn't set up all of the STRUCT internals...
		keys.Initialize(collection.GetAllocator(), types);
	}

	//	TODO: Parallelise on mask entry boundaries
	const idx_t block_begin = 0;
	const auto block_end = collection.ChunkCount();
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
	void ExecuteTask(ExecutionContext &context, DataChunk &chunk);

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
	void Sink(ExecutionContext &context);
	void Finalize(ExecutionContext &context);
	void GetData(ExecutionContext &context, DataChunk &chunk);

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

WindowHashGroup::ExecutorGlobalStates &WindowHashGroup::Initialize(ClientContext &client) {
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

void WindowLocalSourceState::Sink(ExecutionContext &context) {
	D_ASSERT(task);
	D_ASSERT(task->stage == WindowGroupStage::SINK);

	auto &gsink = gsource.gsink;
	const auto &executors = gsink.executors;

	// Create the global state for each function
	// These can be large so we defer building them until we are ready.
	auto &gestates = window_hash_group->Initialize(context.client);

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
			executors[w]->Sink(context, sink_chunk, coll_chunk, input_idx, *gestates[w], *local_states[w]);
		}

		window_hash_group->sunk += input_chunk.size();
	}
	scanner.reset();
}

void WindowLocalSourceState::Finalize(ExecutionContext &context) {
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
		executors[w]->Finalize(context, *gestates[w], *local_states[w], window_hash_group->collection);
	}

	//	Mark this range as done
	window_hash_group->finalized += (task->end_idx - task->begin_idx);
	task->begin_idx = task->end_idx;
}

WindowLocalSourceState::WindowLocalSourceState(WindowGlobalSourceState &gsource)
    : gsource(gsource), batch_index(0), coll_exec(gsource.client), sink_exec(gsource.client),
      eval_exec(gsource.client) {
	auto &gsink = gsource.gsink;
	auto &global_partition = *gsink.global_partition;

	vector<LogicalType> output_types;
	for (auto &wexec : gsink.executors) {
		auto &wexpr = wexec->wexpr;
		output_types.emplace_back(wexpr.return_type);
	}
	output_chunk.Initialize(global_partition.allocator, output_types);

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
		auto &window_hash_group = gsink.window_hash_groups[group_idx];
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

		auto &window_hash_group = gsink.window_hash_groups[group_idx];
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
	auto &finished_hash_group = gsink.window_hash_groups[group_idx];
	D_ASSERT(finished_hash_group);

	if (++finished_hash_group->completed >= finished_hash_group->GetTaskCount()) {
		finished_hash_group.reset();
		//	Remove it from the active groups
		auto &v = active_groups;
		v.erase(std::remove(v.begin(), v.end(), group_idx), v.end());
	}
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

void WindowLocalSourceState::ExecuteTask(ExecutionContext &context, DataChunk &result) {
	auto &gsink = gsource.gsink;

	// Update the hash group
	window_hash_group = gsink.window_hash_groups[task->group_idx].get();

	// Process the new state
	switch (task->stage) {
	case WindowGroupStage::SINK:
		Sink(context);
		D_ASSERT(TaskFinished());
		break;
	case WindowGroupStage::FINALIZE:
		Finalize(context);
		D_ASSERT(TaskFinished());
		break;
	case WindowGroupStage::GETDATA:
		D_ASSERT(!TaskFinished());
		GetData(context, result);
		break;
	default:
		throw InternalException("Invalid window source state.");
	}

	// Count this task as finished.
	if (TaskFinished()) {
		++gsource.finished;
	}
}

void WindowLocalSourceState::GetData(ExecutionContext &context, DataChunk &result) {
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
		auto &gstate = *gestates[expr_idx];
		auto &lstate = *local_states[expr_idx];
		auto &result = output_chunk.data[expr_idx];
		if (eval_chunk.data.empty()) {
			eval_chunk.SetCardinality(input_chunk);
		} else {
			eval_chunk.Reset();
			eval_exec.Execute(input_chunk, eval_chunk);
		}
		executor.Evaluate(context, position, eval_chunk, result, lstate, gstate);
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
	const auto returned = gsource.returned.load();

	auto &gsink = gsource.gsink;
	const auto count = gsink.global_partition->count.load();
	ProgressData res;
	if (count) {
		res.done = double(returned);
		res.total = double(count);
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
                                         OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<WindowGlobalSourceState>();
	auto &lsource = input.local_state.Cast<WindowLocalSourceState>();

	while (gsource.HasUnfinishedTasks() && chunk.size() == 0) {
		if (!lsource.TaskFinished() || lsource.TryAssignTask()) {
			try {
				lsource.ExecuteTask(context, chunk);
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
				return gsource.BlockSource(guard, input.interrupt_state);
			}
		}
	}

	gsource.returned += chunk.size();

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
