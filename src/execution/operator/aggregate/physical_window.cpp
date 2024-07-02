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

	HashGroupPtr hash_group;
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
class WindowPartitionSourceState;

class WindowGlobalSourceState : public GlobalSourceState {
public:
	using HashGroupSourcePtr = unique_ptr<WindowPartitionSourceState>;
	using ScannerPtr = unique_ptr<RowDataCollectionScanner>;
	using Task = std::pair<WindowPartitionSourceState *, ScannerPtr>;

	WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p);

	//! Get the next task
	Task NextTask(idx_t hash_bin);

	//! Context for executing computations
	ClientContext &context;
	//! All the sunk data
	WindowGlobalSinkState &gsink;
	//! The next group to build.
	atomic<idx_t> next_build;
	//! The built groups
	vector<HashGroupSourcePtr> built;
	//! Serialise access to the built hash groups
	mutable mutex built_lock;
	//! The number of unfinished tasks
	atomic<idx_t> tasks_remaining;
	//! The number of rows returned
	atomic<idx_t> returned;

public:
	idx_t MaxThreads() override {
		return tasks_remaining;
	}

private:
	Task CreateTask(idx_t hash_bin);
	Task StealWork();
};

WindowGlobalSourceState::WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p)
    : context(context_p), gsink(gsink_p), next_build(0), tasks_remaining(0), returned(0) {
	auto &gpart = gsink.global_partition;
	auto &window_hash_groups = gsink.global_partition->window_hash_groups;

	if (window_hash_groups.empty()) {
		//	OVER()
		if (gpart->rows) {
			tasks_remaining += gpart->rows->blocks.size();
		}
		if (tasks_remaining) {
			// We need to construct the single WindowHashGroup here because the sort tasks will not be run.
			built.resize(1);
			window_hash_groups.emplace_back(make_uniq<WindowHashGroup>(gsink, idx_t(0)));
		}
	} else {
		built.resize(window_hash_groups.size());
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
			tasks_remaining += block_count;

			window_hash_group->batch_base = batch_base;
			batch_base += block_count;
		}
	}
}

// Per-bin evaluation state (build and evaluate)
class WindowPartitionSourceState {
public:
	using WindowHashGroupPtr = unique_ptr<WindowHashGroup>;

	WindowPartitionSourceState(ClientContext &context, WindowGlobalSourceState &gsource)
	    : context(context), op(gsource.gsink.op), gsource(gsource), read_block_idx(0), unscanned(0) {
	}

	unique_ptr<RowDataCollectionScanner> GetScanner() const;
	void BuildPartition(WindowGlobalSinkState &gstate, const idx_t hash_bin);

	ClientContext &context;
	const PhysicalWindow &op;
	WindowGlobalSourceState &gsource;

	//! The bin number
	WindowHashGroupPtr window_hash_group;

	//! The next block to read.
	mutable atomic<idx_t> read_block_idx;
	//! The number of remaining unscanned blocks.
	atomic<idx_t> unscanned;
};

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
		heap = make_uniq<RowDataCollection>(buffer_manager, buffer_manager.GetBlockSize(), 1U, true);
	}
	heap->count = std::accumulate(heap->blocks.begin(), heap->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
}

unique_ptr<RowDataCollectionScanner> WindowPartitionSourceState::GetScanner() const {
	auto &gsink = *gsource.gsink.global_partition;
	const auto hash_bin = window_hash_group->hash_bin;
	auto &rows = window_hash_group->rows;
	if ((gsink.rows && !hash_bin) || hash_bin < gsink.hash_groups.size()) {
		const auto block_idx = read_block_idx++;
		if (block_idx >= rows->blocks.size()) {
			return nullptr;
		}
		--gsource.tasks_remaining;
		return window_hash_group->GetEvaluateScanner(block_idx);
	}
	return nullptr;
}

WindowHashGroup::WindowHashGroup(WindowGlobalSinkState &gstate, const idx_t hash_bin_p)
    : hash_bin(hash_bin_p), batch_base(0) {
	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	auto &gpart = *gstate.global_partition;
	layout.Initialize(gpart.payload_types);
	idx_t count = 0;
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

	// Create the executor state for each function
	for (auto &wexec : executors) {
		auto &wexpr = wexec->wexpr;
		auto &order_mask = order_masks[wexpr.partitions.size() + wexpr.orders.size()];
		gestates.emplace_back(wexec->GetGlobalState(count, partition_mask, order_mask));
	}
}

void WindowPartitionSourceState::BuildPartition(WindowGlobalSinkState &gsink, const idx_t hash_bin_p) {
	auto &gpart = *gsink.global_partition;
	window_hash_group = std::move(gpart.window_hash_groups[hash_bin_p]);
	const auto &executors = gsink.executors;
	auto &gestates = window_hash_group->gestates;

	//	First pass over the input without flushing
	DataChunk input_chunk;
	input_chunk.Initialize(gpart.allocator, gpart.payload_types);
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

	//	Start the block countdown
	unscanned = window_hash_group->rows->blocks.size();
}

// Per-thread scan state
class WindowLocalSourceState : public LocalSourceState {
public:
	using ReadStatePtr = unique_ptr<WindowExecutorLocalState>;
	using ReadStates = vector<ReadStatePtr>;

	explicit WindowLocalSourceState(WindowGlobalSourceState &gsource);
	void UpdateBatchIndex();
	bool NextPartition();
	void Scan(DataChunk &chunk);

	//! The shared source state
	WindowGlobalSourceState &gsource;
	//! The current bin being processed
	idx_t hash_bin;
	//! The current batch index (for output reordering)
	idx_t batch_index;
	//! The current source being processed
	optional_ptr<WindowPartitionSourceState> partition_source;
	//! The read cursor
	unique_ptr<RowDataCollectionScanner> scanner;
	//! Buffer for the inputs
	DataChunk input_chunk;
	//! Executor read states.
	ReadStates read_states;
	//! Buffer for window results
	DataChunk output_chunk;
};

WindowLocalSourceState::WindowLocalSourceState(WindowGlobalSourceState &gsource)
    : gsource(gsource), hash_bin(gsource.built.size()), batch_index(0) {
	auto &gsink = gsource.gsink;
	auto &global_partition = *gsink.global_partition;

	input_chunk.Initialize(global_partition.allocator, global_partition.payload_types);

	vector<LogicalType> output_types;
	for (auto &wexec : gsink.executors) {
		auto &wexpr = wexec->wexpr;
		output_types.emplace_back(wexpr.return_type);
	}
	output_chunk.Initialize(Allocator::Get(gsource.context), output_types);
}

WindowGlobalSourceState::Task WindowGlobalSourceState::CreateTask(idx_t hash_bin) {
	//	Build outside the lock so no one tries to steal before we are done.
	auto partition_source = make_uniq<WindowPartitionSourceState>(context, *this);
	partition_source->BuildPartition(gsink, hash_bin);
	Task result(partition_source.get(), partition_source->GetScanner());

	//	Is there any data to scan?
	if (result.second) {
		lock_guard<mutex> built_guard(built_lock);
		built[hash_bin] = std::move(partition_source);

		return result;
	}

	return Task();
}

WindowGlobalSourceState::Task WindowGlobalSourceState::StealWork() {
	for (idx_t hash_bin = 0; hash_bin < built.size(); ++hash_bin) {
		lock_guard<mutex> built_guard(built_lock);
		auto &partition_source = built[hash_bin];
		if (!partition_source) {
			continue;
		}

		Task result(partition_source.get(), partition_source->GetScanner());

		//	Is there any data to scan?
		if (result.second) {
			return result;
		}
	}

	//	Nothing to steal
	return Task();
}

WindowGlobalSourceState::Task WindowGlobalSourceState::NextTask(idx_t hash_bin) {
	auto &window_hash_groups = gsink.global_partition->window_hash_groups;
	const auto bin_count = built.size();

	//	Flush unneeded data
	if (hash_bin < bin_count) {
		//	Lock and delete when all blocks have been scanned
		//	We do this here instead of in NextScan so the WindowLocalSourceState
		//	has a chance to delete its state objects first,
		//	which may reference the partition_source

		//	Delete data outside the lock in case it is slow
		HashGroupSourcePtr killed;
		lock_guard<mutex> built_guard(built_lock);
		auto &partition_source = built[hash_bin];
		if (partition_source && !partition_source->unscanned) {
			killed = std::move(partition_source);
		}
	}

	hash_bin = next_build++;
	if (hash_bin < bin_count) {
		//	Find a non-empty hash group.
		for (; hash_bin < built.size(); hash_bin = next_build++) {
			auto &window_hash_group = window_hash_groups[hash_bin];
			if (!window_hash_group) {
				continue;
			}
			auto &rows = window_hash_group->rows;
			if (rows && rows->count) {
				auto result = CreateTask(hash_bin);
				if (result.second) {
					return result;
				}
			}
		}

		//	OVER() doesn't have a hash_group
		if (window_hash_groups.empty()) {
			auto result = CreateTask(hash_bin);
			if (result.second) {
				return result;
			}
		}
	}

	//	Work stealing
	while (!context.interrupted && tasks_remaining) {
		auto result = StealWork();
		if (result.second) {
			return result;
		}

		//	If there is nothing to steal but there are unfinished partitions,
		//	yield until any pending builds are done.
		TaskScheduler::YieldThread();
	}

	return Task();
}

void WindowLocalSourceState::UpdateBatchIndex() {
	D_ASSERT(partition_source);
	D_ASSERT(scanner.get());

	const auto &window_hash_group = partition_source->window_hash_group;
	batch_index = window_hash_group->batch_base;
	batch_index += scanner->BlockIndex();
}

bool WindowLocalSourceState::NextPartition() {
	//	Release old states before the source
	scanner.reset();
	read_states.clear();

	//	Get a partition_source that is not finished
	while (!scanner) {
		auto task = gsource.NextTask(hash_bin);
		if (!task.first) {
			return false;
		}
		partition_source = task.first;
		scanner = std::move(task.second);
		hash_bin = partition_source->window_hash_group->hash_bin;
		UpdateBatchIndex();
	}

	const auto &executors = gsource.gsink.executors;
	auto &gestates = partition_source->window_hash_group->gestates;
	for (idx_t w = 0; w < executors.size(); ++w) {
		read_states.emplace_back(executors[w]->GetLocalState(*gestates[w]));
	}

	return true;
}

void WindowLocalSourceState::Scan(DataChunk &result) {
	D_ASSERT(scanner);
	if (!scanner->Remaining()) {
		lock_guard<mutex> built_guard(gsource.built_lock);
		--partition_source->unscanned;
		scanner = partition_source->GetScanner();

		if (!scanner) {
			partition_source = nullptr;
			read_states.clear();
			return;
		}

		UpdateBatchIndex();
	}

	const auto position = scanner->Scanned();
	input_chunk.Reset();
	scanner->Scan(input_chunk);

	const auto &executors = gsource.gsink.executors;
	auto &gestates = partition_source->window_hash_group->gestates;
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
	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner) {
			if (!lsource.NextPartition()) {
				return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
			}
		}

		lsource.Scan(chunk);
		gsource.returned += chunk.size();
	}

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
