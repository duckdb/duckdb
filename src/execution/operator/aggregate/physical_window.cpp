#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/partition_state.hpp"

#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/row/row_data_collection_scanner.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

//	Global sink state
class WindowGlobalSinkState;

class WindowHashGroup {
public:
	using HashGroupPtr = unique_ptr<PartitionGlobalHashGroup>;
	using OrderMasks = PartitionGlobalHashGroup::OrderMasks;
	using ExecutorGlobalStatePtr = unique_ptr<WindowExecutorGlobalState>;
	using ExecutorGlobalStates = vector<ExecutorGlobalStatePtr>;

	WindowHashGroup(WindowGlobalSinkState &gstate, const idx_t hash_bin_p);

	// Scan all of the blocks during the build phase
	unique_ptr<RowDataCollectionScanner> GetBuildScanner() const {
		if (!rows) {
			return nullptr;
		}
		return make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, external, false);
	}

	// Scan a single block during the evaluate phase
	unique_ptr<RowDataCollectionScanner> GetEvaluateScanner(idx_t block_idx) const {
		//	Second pass can flush
		D_ASSERT(rows);
		return make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, external, block_idx, true);
	}

	//! The hash partition data
	HashGroupPtr hash_group;
	//! The size of the group
	idx_t count = 0;
	//! The generated input chunks
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> heap;
	RowLayout layout;
	//! The partition boundary mask
	ValidityMask partition_mask;
	//! The order boundary mask
	OrderMasks order_masks;
	//! External paging
	bool external;
	//! The function global states for this hash group
	ExecutorGlobalStates gestates;

	//! The bin number
	idx_t hash_bin;
	//! Single threading lock
	std::mutex lock;
	//! The group has been built
	bool built = false;
	//! The number of tasks left before we should be deleted
	std::atomic<idx_t> tasks_remaining;
	//! The output ordering batch index this hash group starts at
	idx_t batch_base;

private:
	void MaterializeSortedData();
};

class WindowPartitionGlobalSinkState;

class WindowGlobalSinkState : public GlobalSinkState {
public:
	using ExecutorPtr = unique_ptr<WindowExecutor>;
	using Executors = vector<ExecutorPtr>;

	WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &context);

	//! Parent operator
	const PhysicalWindow &op;
	//! Execution context
	ClientContext &context;
	//! The partitioned sunk data
	unique_ptr<WindowPartitionGlobalSinkState> global_partition;
	//! The execution functions
	Executors executors;
};

class WindowPartitionGlobalSinkState : public PartitionGlobalSinkState {
public:
	using WindowHashGroupPtr = unique_ptr<WindowHashGroup>;

	WindowPartitionGlobalSinkState(WindowGlobalSinkState &gsink, const BoundWindowExpression &wexpr)
	    : PartitionGlobalSinkState(gsink.context, wexpr.partitions, wexpr.orders, gsink.op.children[0]->types,
	                               wexpr.partitions_stats, gsink.op.estimated_cardinality),
	      gsink(gsink) {
	}
	~WindowPartitionGlobalSinkState() override = default;

	void OnBeginMerge() override {
		PartitionGlobalSinkState::OnBeginMerge();
		window_hash_groups.resize(hash_groups.size());
	}

	void OnSortedPartition(const idx_t group_idx) override {
		PartitionGlobalSinkState::OnSortedPartition(group_idx);
		window_hash_groups[group_idx] = make_uniq<WindowHashGroup>(gsink, group_idx);
	}

	//! Operator global sink state
	WindowGlobalSinkState &gsink;
	//! The sorted hash groups
	vector<WindowHashGroupPtr> window_hash_groups;
};

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	WindowLocalSinkState(ClientContext &context, const WindowGlobalSinkState &gstate)
	    : local_partition(context, *gstate.global_partition) {
	}

	void Sink(DataChunk &input_chunk) {
		local_partition.Sink(input_chunk);
	}

	void Combine() {
		local_partition.Combine();
	}

	PartitionLocalSinkState local_partition;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list_p,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list_p)),
      order_idx(0), is_order_dependent(false) {

	idx_t max_orders = 0;
	for (idx_t i = 0; i < select_list.size(); ++i) {
		auto &expr = select_list[i];
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_WINDOW);
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

static unique_ptr<WindowExecutor> WindowExecutorFactory(BoundWindowExpression &wexpr, ClientContext &context,
                                                        WindowAggregationMode mode) {
	switch (wexpr.type) {
	case ExpressionType::WINDOW_AGGREGATE:
		return make_uniq<WindowAggregateExecutor>(wexpr, context, mode);
	case ExpressionType::WINDOW_ROW_NUMBER:
		return make_uniq<WindowRowNumberExecutor>(wexpr, context);
	case ExpressionType::WINDOW_RANK_DENSE:
		return make_uniq<WindowDenseRankExecutor>(wexpr, context);
	case ExpressionType::WINDOW_RANK:
		return make_uniq<WindowRankExecutor>(wexpr, context);
	case ExpressionType::WINDOW_PERCENT_RANK:
		return make_uniq<WindowPercentRankExecutor>(wexpr, context);
	case ExpressionType::WINDOW_CUME_DIST:
		return make_uniq<WindowCumeDistExecutor>(wexpr, context);
	case ExpressionType::WINDOW_NTILE:
		return make_uniq<WindowNtileExecutor>(wexpr, context);
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		return make_uniq<WindowLeadLagExecutor>(wexpr, context);
	case ExpressionType::WINDOW_FIRST_VALUE:
		return make_uniq<WindowFirstValueExecutor>(wexpr, context);
	case ExpressionType::WINDOW_LAST_VALUE:
		return make_uniq<WindowLastValueExecutor>(wexpr, context);
	case ExpressionType::WINDOW_NTH_VALUE:
		return make_uniq<WindowNthValueExecutor>(wexpr, context);
		break;
	default:
		throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr.type));
	}
}

WindowGlobalSinkState::WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &context)
    : op(op), context(context) {

	D_ASSERT(op.select_list[op.order_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
	auto &wexpr = op.select_list[op.order_idx]->Cast<BoundWindowExpression>();

	const auto mode = DBConfig::GetConfig(context).options.window_mode;
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
		auto wexec = WindowExecutorFactory(wexpr, context, mode);
		executors.emplace_back(std::move(wexec));
	}

	global_partition = make_uniq<WindowPartitionGlobalSinkState>(*this, wexpr);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalWindow::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();

	lstate.Sink(chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalWindow::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();
	lstate.Combine();

	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowLocalSinkState>(context.client, gstate);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<WindowGlobalSinkState>(*this, context);
}

SinkFinalizeType PhysicalWindow::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<WindowGlobalSinkState>();

	//	Did we get any data?
	if (!state.global_partition->count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Do we have any sorting to schedule?
	if (state.global_partition->rows) {
		D_ASSERT(!state.global_partition->grouping_data);
		return state.global_partition->rows->count ? SinkFinalizeType::READY : SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Find the first group to sort
	if (!state.global_partition->HasMergeTasks()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Schedule all the sorts for maximum thread utilisation
	auto new_event = make_shared_ptr<PartitionMergeEvent>(*state.global_partition, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	using ScannerPtr = unique_ptr<RowDataCollectionScanner>;

	struct Task {
		Task(idx_t group_idx, idx_t max_idx) : group_idx(group_idx), max_idx(max_idx) {
		}
		idx_t group_idx;
		idx_t max_idx;
		idx_t begin_idx = 0;
		idx_t end_idx = 0;
	};
	using TaskPtr = optional_ptr<Task>;

	WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p);

	//! Get the next task
	TaskPtr NextTask();
	//! Finish a task
	void FinishTask(TaskPtr task);

	//! Context for executing computations
	ClientContext &context;
	//! All the sunk data
	WindowGlobalSinkState &gsink;
	//! The list of tasks
	vector<Task> tasks;
	//! The the next task
	std::atomic<idx_t> next_task;
	//! The number of rows returned
	atomic<idx_t> returned;

public:
	idx_t MaxThreads() override {
		return tasks.size();
	}
};

WindowGlobalSourceState::WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p)
    : context(context_p), gsink(gsink_p), next_task(0), returned(0) {
	auto &gpart = gsink.global_partition;
	auto &window_hash_groups = gsink.global_partition->window_hash_groups;

	if (window_hash_groups.empty()) {
		//	OVER()
		if (gpart->rows && !gpart->rows->blocks.empty()) {
			// We need to construct the single WindowHashGroup here because the sort tasks will not be run.
			window_hash_groups.emplace_back(make_uniq<WindowHashGroup>(gsink, idx_t(0)));
		}
	} else {
		idx_t batch_base = 0;
		for (auto &window_hash_group : window_hash_groups) {
			if (!window_hash_group) {
				continue;
			}
			auto &rows = window_hash_group->rows;
			if (!rows) {
				continue;
			}

			const auto block_count = window_hash_group->rows->blocks.size();
			window_hash_group->batch_base = batch_base;
			batch_base += block_count;
		}
	}

	//    Sort the groups from largest to smallest
	if (window_hash_groups.empty()) {
		return;
	}

	using PartitionBlock = std::pair<idx_t, idx_t>;
	vector<PartitionBlock> partition_blocks;
	for (idx_t group_idx = 0; group_idx < window_hash_groups.size(); ++group_idx) {
		auto &window_hash_group = window_hash_groups[group_idx];
		partition_blocks.emplace_back(window_hash_group->rows->blocks.size(), group_idx);
	}
	std::sort(partition_blocks.begin(), partition_blocks.end(), std::greater<PartitionBlock>());

	//	Schedule the largest group on as many threads as possible
	const auto threads = idx_t(TaskScheduler::GetScheduler(context).NumberOfThreads());
	const auto &max_block = partition_blocks.front();
	const auto per_thread = (max_block.first + threads - 1) / threads;

	for (const auto &b : partition_blocks) {
		for (Task task(b.second, b.first); task.begin_idx < task.max_idx; task.begin_idx += per_thread) {
			task.end_idx = MinValue<idx_t>(task.begin_idx + per_thread, task.max_idx);
			tasks.emplace_back(task);
			window_hash_groups[task.group_idx]->tasks_remaining++;
		}
	}
}

void WindowHashGroup::MaterializeSortedData() {
	auto &global_sort_state = *hash_group->global_sort;
	if (global_sort_state.sorted_blocks.empty()) {
		return;
	}

	// scan the sorted row data
	D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
	auto &sb = *global_sort_state.sorted_blocks[0];

	// Free up some memory before allocating more
	sb.radix_sorting_data.clear();
	sb.blob_sorting_data = nullptr;

	// Move the sorting row blocks into our RDCs
	auto &buffer_manager = global_sort_state.buffer_manager;
	auto &sd = *sb.payload_data;

	// Data blocks are required
	D_ASSERT(!sd.data_blocks.empty());
	auto &block = sd.data_blocks[0];
	rows = make_uniq<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
	rows->blocks = std::move(sd.data_blocks);
	rows->count = std::accumulate(rows->blocks.begin(), rows->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });

	// Heap blocks are optional, but we want both for iteration.
	if (!sd.heap_blocks.empty()) {
		auto &block = sd.heap_blocks[0];
		heap = make_uniq<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
		heap->blocks = std::move(sd.heap_blocks);
		hash_group.reset();
	} else {
		heap = make_uniq<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1U, true);
	}
	heap->count = std::accumulate(heap->blocks.begin(), heap->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
}

WindowHashGroup::WindowHashGroup(WindowGlobalSinkState &gstate, const idx_t hash_bin_p)
    : hash_bin(hash_bin_p), tasks_remaining(0), batch_base(0) {
	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	auto &gpart = *gstate.global_partition;
	layout.Initialize(gpart.payload_types);
	if (hash_bin < gpart.hash_groups.size() && gpart.hash_groups[hash_bin]) {
		count = gpart.hash_groups[hash_bin]->count;
	} else if (gpart.rows && !hash_bin) {
		count = gpart.count;
	} else {
		return;
	}

	//	Initialise masks to false
	partition_mask.Initialize(count);
	partition_mask.SetAllInvalid(count);

	const auto &executors = gstate.executors;
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
	external = gpart.external;
	if (gpart.rows && !hash_bin) {
		// Simple mask
		partition_mask.SetValidUnsafe(0);
		for (auto &order_mask : order_masks) {
			order_mask.second.SetValidUnsafe(0);
		}
		//	No partition - align the heap blocks with the row blocks
		rows = gpart.rows->CloneEmpty(gpart.rows->keep_pinned);
		heap = gpart.strings->CloneEmpty(gpart.strings->keep_pinned);
		RowDataCollectionScanner::AlignHeapBlocks(*rows, *heap, *gpart.rows, *gpart.strings, layout);
		external = true;
	} else if (hash_bin < gpart.hash_groups.size()) {
		// Overwrite the collections with the sorted data
		D_ASSERT(gpart.hash_groups[hash_bin].get());
		hash_group = std::move(gpart.hash_groups[hash_bin]);
		hash_group->ComputeMasks(partition_mask, order_masks);
		external = hash_group->global_sort->external;
		MaterializeSortedData();
	} else {
		return;
	}
}

// Per-thread scan state
class WindowLocalSourceState : public LocalSourceState {
public:
	using ReadStatePtr = unique_ptr<WindowExecutorLocalState>;
	using ReadStates = vector<ReadStatePtr>;
	using Task = WindowGlobalSourceState::Task;
	using TaskPtr = optional_ptr<Task>;

	explicit WindowLocalSourceState(WindowGlobalSourceState &gsource);
	void FinishPartition(TaskPtr prev_task);
	void NextPartition();
	void BuildPartition();
	void Scan(DataChunk &chunk);

	//! The shared source state
	WindowGlobalSourceState &gsource;
	//! The current batch index (for output reordering)
	idx_t batch_index;
	//! The task this thread is working on
	TaskPtr task;
	//! The current source being processed
	optional_ptr<WindowHashGroup> window_hash_group;
	//! The scan cursor
	unique_ptr<RowDataCollectionScanner> scanner;
	//! Buffer for the inputs
	DataChunk input_chunk;
	//! Executor read states.
	ReadStates read_states;
	//! Buffer for window results
	DataChunk output_chunk;
};

void WindowLocalSourceState::BuildPartition() {
	auto &gsink = gsource.gsink;
	auto &gpart = *gsink.global_partition;
	window_hash_group = gpart.window_hash_groups[task->group_idx].get();

	//	Single-threaded building for now.
	lock_guard<mutex> built_guard(window_hash_group->lock);
	if (window_hash_group->built) {
		return;
	}

	// Create the executor state for each function
	// These can be large so we defer building them until we are ready.
	const auto count = window_hash_group->count;
	auto &gestates = window_hash_group->gestates;
	const auto &partition_mask = window_hash_group->partition_mask;
	auto &order_masks = window_hash_group->order_masks;
	const auto &executors = gsink.executors;
	for (auto &wexec : executors) {
		auto &wexpr = wexec->wexpr;
		auto &order_mask = order_masks[wexpr.partitions.size() + wexpr.orders.size()];
		gestates.emplace_back(wexec->GetGlobalState(count, partition_mask, order_mask));
	}

	//	First pass over the input without flushing
	auto scanner = window_hash_group->GetBuildScanner();
	if (!scanner) {
		return;
	}
	idx_t input_idx = 0;
	while (true) {
		input_chunk.Reset();
		scanner->Scan(input_chunk);
		if (input_chunk.size() == 0) {
			break;
		}

		//	TODO: Parallelization opportunity
		for (idx_t w = 0; w < executors.size(); ++w) {
			executors[w]->Sink(input_chunk, input_idx, scanner->Count(), *gestates[w]);
		}
		input_idx += input_chunk.size();
	}

	//	TODO: Parallelization opportunity
	for (idx_t w = 0; w < executors.size(); ++w) {
		executors[w]->Finalize(*gestates[w]);
	}

	// External scanning assumes all blocks are swizzled.
	scanner->ReSwizzle();

	window_hash_group->built = true;
}

WindowLocalSourceState::WindowLocalSourceState(WindowGlobalSourceState &gsource) : gsource(gsource), batch_index(0) {
	auto &gsink = gsource.gsink;
	auto &global_partition = *gsink.global_partition;

	input_chunk.Initialize(global_partition.allocator, global_partition.payload_types);

	vector<LogicalType> output_types;
	for (auto &wexec : gsink.executors) {
		auto &wexpr = wexec->wexpr;
		output_types.emplace_back(wexpr.return_type);
	}
	output_chunk.Initialize(global_partition.allocator, output_types);
}

WindowGlobalSourceState::TaskPtr WindowGlobalSourceState::NextTask() {
	const auto task_idx = next_task++;
	if (task_idx < tasks.size()) {
		return &tasks[task_idx];
	}
	return nullptr;
}

void WindowGlobalSourceState::FinishTask(TaskPtr task) {
	if (!task) {
		return;
	}

	auto &gpart = *gsink.global_partition;
	auto &finished_hash_group = gpart.window_hash_groups[task->group_idx];
	D_ASSERT(finished_hash_group);

	if (!--finished_hash_group->tasks_remaining) {
		finished_hash_group.reset();
	}
}

void WindowLocalSourceState::NextPartition() {
	D_ASSERT(window_hash_group);
	const auto &executors = gsource.gsink.executors;
	auto &gestates = window_hash_group->gestates;
	for (idx_t w = 0; w < executors.size(); ++w) {
		read_states.emplace_back(executors[w]->GetLocalState(*gestates[w]));
	}
}

void WindowLocalSourceState::FinishPartition(TaskPtr prev_task) {
	scanner.reset();
	read_states.clear();

	gsource.FinishTask(prev_task);
}

void WindowLocalSourceState::Scan(DataChunk &result) {
	// Are we done with this scanner?
	if (scanner && !scanner->Remaining()) {
		scanner.reset();
		++task->begin_idx;
	}

	//	Are we done with this task?
	if (!task || task->begin_idx >= task->end_idx) {
		auto prev_task = task;
		task = gsource.NextTask();

		// Did we start a new group?
		const bool new_group = (!task || !prev_task || prev_task->group_idx != task->group_idx);
		if (new_group) {
			FinishPartition(prev_task);
		}

		// Are we done?
		if (!task) {
			return;
		}

		// Did we start a new hash group?
		if (new_group) {
			BuildPartition();
			NextPartition();
		}
	}

	if (!scanner) {
		scanner = window_hash_group->GetEvaluateScanner(task->begin_idx);
		batch_index = window_hash_group->batch_base + task->begin_idx;
	}

	const auto position = scanner->Scanned();
	input_chunk.Reset();
	scanner->Scan(input_chunk);

	const auto &executors = gsource.gsink.executors;
	auto &gestates = window_hash_group->gestates;
	output_chunk.Reset();
	for (idx_t expr_idx = 0; expr_idx < executors.size(); ++expr_idx) {
		auto &executor = *executors[expr_idx];
		auto &gstate = *gestates[expr_idx];
		auto &lstate = *read_states[expr_idx];
		auto &result = output_chunk.data[expr_idx];
		executor.Evaluate(position, input_chunk, result, lstate, gstate);
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
	result.Verify();
}

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gsource_p) const {
	auto &gsource = gsource_p.Cast<WindowGlobalSourceState>();
	return make_uniq<WindowLocalSourceState>(gsource);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowGlobalSourceState>(context, gsink);
}

bool PhysicalWindow::SupportsBatchIndex() const {
	//	We can only preserve order for single partitioning
	//	or work stealing causes out of order batch numbers
	auto &wexpr = select_list[order_idx]->Cast<BoundWindowExpression>();
	return wexpr.partitions.empty() && !wexpr.orders.empty();
}

OrderPreservationType PhysicalWindow::SourceOrder() const {
	return SupportsBatchIndex() ? OrderPreservationType::FIXED_ORDER : OrderPreservationType::NO_ORDER;
}

double PhysicalWindow::GetProgress(ClientContext &context, GlobalSourceState &gsource_p) const {
	auto &gsource = gsource_p.Cast<WindowGlobalSourceState>();
	const auto returned = gsource.returned.load();

	auto &gsink = gsource.gsink;
	const auto count = gsink.global_partition->count.load();
	return count ? (double(returned) / double(count)) : -1;
}

idx_t PhysicalWindow::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                    LocalSourceState &lstate_p) const {
	auto &lstate = lstate_p.Cast<WindowLocalSourceState>();
	return lstate.batch_index;
}

SourceResultType PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &gsource = input.global_state.Cast<WindowGlobalSourceState>();
	auto &lsource = input.local_state.Cast<WindowLocalSourceState>();

	lsource.Scan(chunk);
	gsource.returned += chunk.size();

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb
