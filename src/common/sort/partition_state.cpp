#include "duckdb/common/sort/partition_state.hpp"

#include "duckdb/common/types/column_data_consumer.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/event.hpp"

#include <numeric>

namespace duckdb {

PartitionGlobalHashGroup::PartitionGlobalHashGroup(BufferManager &buffer_manager, const Orders &partitions,
                                                   const Orders &orders, const Types &payload_types, bool external)
    : count(0) {

	RowLayout payload_layout;
	payload_layout.Initialize(payload_types);
	global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
	global_sort->external = external;

	partition_layout = global_sort->sort_layout.GetPrefixComparisonLayout(partitions.size());
}

void PartitionGlobalHashGroup::ComputeMasks(ValidityMask &partition_mask, ValidityMask &order_mask) {
	D_ASSERT(count > 0);

	//	Set up a comparator for the partition subset
	const auto partition_size = partition_layout.comparison_size;

	SBIterator prev(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator curr(*global_sort, ExpressionType::COMPARE_LESSTHAN);

	partition_mask.SetValidUnsafe(0);
	order_mask.SetValidUnsafe(0);
	for (++curr; curr.GetIndex() < count; ++curr) {
		//	Compare the partition subset first because if that differs, then so does the full ordering
		int part_cmp = 0;
		if (partition_layout.all_constant) {
			part_cmp = FastMemcmp(prev.entry_ptr, curr.entry_ptr, partition_size);
		} else {
			part_cmp = Comparators::CompareTuple(prev.scan, curr.scan, prev.entry_ptr, curr.entry_ptr, partition_layout,
			                                     prev.external);
		}

		if (part_cmp) {
			partition_mask.SetValidUnsafe(curr.GetIndex());
			order_mask.SetValidUnsafe(curr.GetIndex());
		} else if (prev.Compare(curr)) {
			order_mask.SetValidUnsafe(curr.GetIndex());
		}
		++prev;
	}
}

PartitionGlobalSinkState::PartitionGlobalSinkState(ClientContext &context,
                                                   const vector<unique_ptr<Expression>> &partitions_p,
                                                   const vector<BoundOrderByNode> &orders_p, const Types &payload_types,
                                                   const vector<unique_ptr<BaseStatistics>> &partitions_stats,
                                                   idx_t estimated_cardinality)
    : context(context), buffer_manager(BufferManager::GetBufferManager(context)), allocator(Allocator::Get(context)),
      payload_types(payload_types), memory_per_thread(0), count(0) {

	// we sort by both 1) partition by expression list and 2) order by expressions
	const auto partition_cols = partitions_p.size();
	for (idx_t prt_idx = 0; prt_idx < partition_cols; prt_idx++) {
		auto &pexpr = partitions_p[prt_idx];

		if (partitions_stats.empty() || !partitions_stats[prt_idx]) {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(), nullptr);
		} else {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(),
			                    partitions_stats[prt_idx]->ToUnique());
		}
		partitions.emplace_back(orders.back().Copy());
	}

	for (const auto &order : orders_p) {
		orders.emplace_back(order.Copy());
	}

	memory_per_thread = PhysicalOperator::GetMaxThreadMemory(context);
	external = ClientConfig::GetConfig(context).force_external;

	if (!orders.empty()) {
		grouping_types = payload_types;
		grouping_types.push_back(LogicalType::HASH);

		ResizeGroupingData(estimated_cardinality);
	}
}

void PartitionGlobalSinkState::ResizeGroupingData(idx_t cardinality) {
	//	Have we started to combine? Then just live with it.
	if (grouping_data && !grouping_data->GetPartitions().empty()) {
		return;
	}
	//	Is the average partition size too large?
	const idx_t partition_size = STANDARD_ROW_GROUPS_SIZE;
	const auto bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	auto new_bits = bits ? bits : 4;
	while (new_bits < 10 && (cardinality / RadixPartitioning::NumberOfPartitions(new_bits)) > partition_size) {
		++new_bits;
	}

	// Repartition the grouping data
	if (new_bits != bits) {
		const auto hash_col_idx = payload_types.size();
		grouping_data = make_uniq<RadixPartitionedColumnData>(context, grouping_types, new_bits, hash_col_idx);
	}
}

void PartitionGlobalSinkState::SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// We are done if the local_partition is right sized.
	auto local_radix = (RadixPartitionedColumnData *)local_partition.get();
	if (local_radix->GetRadixBits() == grouping_data->GetRadixBits()) {
		return;
	}

	// If the local partition is now too small, flush it and reallocate
	auto new_partition = grouping_data->CreateShared();
	auto new_append = make_uniq<PartitionedColumnDataAppendState>();
	new_partition->InitializeAppendState(*new_append);

	local_partition->FlushAppendState(*local_append);
	auto &local_groups = local_partition->GetPartitions();
	for (auto &local_group : local_groups) {
		ColumnDataScanState scanner;
		local_group->InitializeScan(scanner);

		DataChunk scan_chunk;
		local_group->InitializeScanChunk(scan_chunk);
		for (scan_chunk.Reset(); local_group->Scan(scanner, scan_chunk); scan_chunk.Reset()) {
			new_partition->Append(*new_append, scan_chunk);
		}
	}

	// The append state has stale pointers to the old local partition, so nuke it from orbit.
	new_partition->FlushAppendState(*new_append);

	local_partition = std::move(new_partition);
	local_append = make_uniq<PartitionedColumnDataAppendState>();
	local_partition->InitializeAppendState(*local_append);
}

void PartitionGlobalSinkState::UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// Make sure grouping_data doesn't change under us.
	lock_guard<mutex> guard(lock);

	if (!local_partition) {
		local_partition = grouping_data->CreateShared();
		local_append = make_uniq<PartitionedColumnDataAppendState>();
		local_partition->InitializeAppendState(*local_append);
		return;
	}

	// 	Grow the groups if they are too big
	ResizeGroupingData(count);

	//	Sync local partition to have the same bit count
	SyncLocalPartition(local_partition, local_append);
}

void PartitionGlobalSinkState::CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	if (!local_partition) {
		return;
	}
	local_partition->FlushAppendState(*local_append);

	// Make sure grouping_data doesn't change under us.
	// Combine has an internal mutex, so this is single-threaded anyway.
	lock_guard<mutex> guard(lock);
	SyncLocalPartition(local_partition, local_append);
	grouping_data->Combine(*local_partition);
}

void PartitionGlobalSinkState::BuildSortState(ColumnDataCollection &group_data, PartitionGlobalHashGroup &hash_group) {
	auto &global_sort = *hash_group.global_sort;

	//	 Set up the sort expression computation.
	vector<LogicalType> sort_types;
	ExpressionExecutor executor(context);
	for (auto &order : orders) {
		auto &oexpr = order.expression;
		sort_types.emplace_back(oexpr->return_type);
		executor.AddExpression(*oexpr);
	}
	DataChunk sort_chunk;
	sort_chunk.Initialize(allocator, sort_types);

	// Copy the data from the group into the sort code.
	LocalSortState local_sort;
	local_sort.Initialize(global_sort, global_sort.buffer_manager);

	//	Strip hash column
	DataChunk payload_chunk;
	payload_chunk.Initialize(allocator, payload_types);

	vector<column_t> column_ids;
	column_ids.reserve(payload_types.size());
	for (column_t i = 0; i < payload_types.size(); ++i) {
		column_ids.emplace_back(i);
	}
	ColumnDataConsumer scanner(group_data, column_ids);
	ColumnDataConsumerScanState chunk_state;
	chunk_state.current_chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	scanner.InitializeScan();
	for (auto chunk_idx = scanner.ChunkCount(); chunk_idx-- > 0;) {
		if (!scanner.AssignChunk(chunk_state)) {
			break;
		}
		scanner.ScanChunk(chunk_state, payload_chunk);

		sort_chunk.Reset();
		executor.Execute(payload_chunk, sort_chunk);

		local_sort.SinkChunk(sort_chunk, payload_chunk);
		if (local_sort.SizeInBytes() > memory_per_thread) {
			local_sort.Sort(global_sort, true);
		}
		scanner.FinishChunk(chunk_state);
	}

	global_sort.AddLocalState(local_sort);

	hash_group.count += group_data.Count();
}

//	Per-thread sink state
PartitionLocalSinkState::PartitionLocalSinkState(ClientContext &context, PartitionGlobalSinkState &gstate_p)
    : gstate(gstate_p), allocator(Allocator::Get(context)), executor(context) {

	vector<LogicalType> group_types;
	for (idx_t prt_idx = 0; prt_idx < gstate.partitions.size(); prt_idx++) {
		auto &pexpr = *gstate.partitions[prt_idx].expression.get();
		group_types.push_back(pexpr.return_type);
		executor.AddExpression(pexpr);
	}
	sort_cols = gstate.orders.size() + group_types.size();

	if (sort_cols) {
		if (!group_types.empty()) {
			// OVER(PARTITION BY...)
			group_chunk.Initialize(allocator, group_types);
		}
		// OVER(...)
		auto payload_types = gstate.payload_types;
		payload_types.emplace_back(LogicalType::HASH);
		payload_chunk.Initialize(allocator, payload_types);
	} else {
		// OVER()
		payload_layout.Initialize(gstate.payload_types);
	}
}

void PartitionLocalSinkState::Hash(DataChunk &input_chunk, Vector &hash_vector) {
	const auto count = input_chunk.size();
	if (group_chunk.ColumnCount() > 0) {
		// OVER(PARTITION BY...) (hash grouping)
		group_chunk.Reset();
		executor.Execute(input_chunk, group_chunk);
		VectorOperations::Hash(group_chunk.data[0], hash_vector, count);
		for (idx_t prt_idx = 1; prt_idx < group_chunk.ColumnCount(); ++prt_idx) {
			VectorOperations::CombineHash(hash_vector, group_chunk.data[prt_idx], count);
		}
	} else {
		// OVER(...) (sorting)
		// Single partition => single hash value
		hash_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto hashes = ConstantVector::GetData<hash_t>(hash_vector);
		hashes[0] = 0;
	}
}

void PartitionLocalSinkState::Sink(DataChunk &input_chunk) {
	gstate.count += input_chunk.size();

	// OVER()
	if (sort_cols == 0) {
		//	No sorts, so build paged row chunks
		if (!rows) {
			const auto entry_size = payload_layout.GetRowWidth();
			const auto capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_SIZE / entry_size) + 1);
			rows = make_uniq<RowDataCollection>(gstate.buffer_manager, capacity, entry_size);
			strings = make_uniq<RowDataCollection>(gstate.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
		}
		const auto row_count = input_chunk.size();
		const auto row_sel = FlatVector::IncrementalSelectionVector();
		Vector addresses(LogicalType::POINTER);
		auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
		const auto prev_rows_blocks = rows->blocks.size();
		auto handles = rows->Build(row_count, key_locations, nullptr, row_sel);
		auto input_data = input_chunk.ToUnifiedFormat();
		RowOperations::Scatter(input_chunk, input_data.get(), payload_layout, addresses, *strings, *row_sel, row_count);
		// Mark that row blocks contain pointers (heap blocks are pinned)
		if (!payload_layout.AllConstant()) {
			D_ASSERT(strings->keep_pinned);
			for (size_t i = prev_rows_blocks; i < rows->blocks.size(); ++i) {
				rows->blocks[i]->block->SetSwizzling("PartitionLocalSinkState::Sink");
			}
		}
		return;
	}

	// OVER(...)
	payload_chunk.Reset();
	auto &hash_vector = payload_chunk.data.back();
	Hash(input_chunk, hash_vector);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}
	payload_chunk.SetCardinality(input_chunk);

	gstate.UpdateLocalPartition(local_partition, local_append);
	local_partition->Append(*local_append, payload_chunk);
}

void PartitionLocalSinkState::Combine() {
	// OVER()
	if (sort_cols == 0) {
		// Only one partition again, so need a global lock.
		lock_guard<mutex> glock(gstate.lock);
		if (gstate.rows) {
			if (rows) {
				gstate.rows->Merge(*rows);
				gstate.strings->Merge(*strings);
				rows.reset();
				strings.reset();
			}
		} else {
			gstate.rows = std::move(rows);
			gstate.strings = std::move(strings);
		}
		return;
	}

	// OVER(...)
	gstate.CombineLocalPartition(local_partition, local_append);
}

PartitionGlobalMergeState::PartitionGlobalMergeState(PartitionGlobalSinkState &sink, GroupDataPtr group_data)
    : sink(sink), group_data(std::move(group_data)), stage(PartitionSortStage::INIT), total_tasks(0), tasks_assigned(0),
      tasks_completed(0) {

	const auto group_idx = sink.hash_groups.size();
	auto new_group = make_uniq<PartitionGlobalHashGroup>(sink.buffer_manager, sink.partitions, sink.orders,
	                                                     sink.payload_types, sink.external);
	sink.hash_groups.emplace_back(std::move(new_group));

	hash_group = sink.hash_groups[group_idx].get();
	global_sort = sink.hash_groups[group_idx]->global_sort.get();
}

void PartitionLocalMergeState::Prepare() {
	auto &global_sort = *merge_state->global_sort;
	merge_state->sink.BuildSortState(*merge_state->group_data, *merge_state->hash_group);
	merge_state->group_data.reset();

	global_sort.PrepareMergePhase();
}

void PartitionLocalMergeState::Merge() {
	auto &global_sort = *merge_state->global_sort;
	MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
	merge_sorter.PerformInMergeRound();
}

void PartitionLocalMergeState::ExecuteTask() {
	switch (stage) {
	case PartitionSortStage::PREPARE:
		Prepare();
		break;
	case PartitionSortStage::MERGE:
		Merge();
		break;
	default:
		throw InternalException("Unexpected PartitionGlobalMergeState in ExecuteTask!");
	}

	merge_state->CompleteTask();
	finished = true;
}

bool PartitionGlobalMergeState::AssignTask(PartitionLocalMergeState &local_state) {
	lock_guard<mutex> guard(lock);

	if (tasks_assigned >= total_tasks) {
		return false;
	}

	local_state.merge_state = this;
	local_state.stage = stage;
	local_state.finished = false;
	tasks_assigned++;

	return true;
}

void PartitionGlobalMergeState::CompleteTask() {
	lock_guard<mutex> guard(lock);

	++tasks_completed;
}

bool PartitionGlobalMergeState::TryPrepareNextStage() {
	lock_guard<mutex> guard(lock);

	if (tasks_completed < total_tasks) {
		return false;
	}

	tasks_assigned = tasks_completed = 0;

	switch (stage) {
	case PartitionSortStage::INIT:
		total_tasks = 1;
		stage = PartitionSortStage::PREPARE;
		return true;

	case PartitionSortStage::PREPARE:
		total_tasks = global_sort->sorted_blocks.size() / 2;
		if (!total_tasks) {
			break;
		}
		stage = PartitionSortStage::MERGE;
		global_sort->InitializeMergeRound();
		return true;

	case PartitionSortStage::MERGE:
		global_sort->CompleteMergeRound(true);
		total_tasks = global_sort->sorted_blocks.size() / 2;
		if (!total_tasks) {
			break;
		}
		global_sort->InitializeMergeRound();
		return true;

	case PartitionSortStage::SORTED:
		break;
	}

	stage = PartitionSortStage::SORTED;

	return false;
}

PartitionGlobalMergeStates::PartitionGlobalMergeStates(PartitionGlobalSinkState &sink) {
	// Schedule all the sorts for maximum thread utilisation
	for (auto &group_data : sink.grouping_data->GetPartitions()) {
		// Prepare for merge sort phase
		if (group_data->Count()) {
			auto state = make_uniq<PartitionGlobalMergeState>(sink, std::move(group_data));
			states.emplace_back(std::move(state));
		}
	}
}

class PartitionMergeTask : public ExecutorTask {
public:
	PartitionMergeTask(shared_ptr<Event> event_p, ClientContext &context_p, PartitionGlobalMergeStates &hash_groups_p)
	    : ExecutorTask(context_p), event(std::move(event_p)), hash_groups(hash_groups_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	shared_ptr<Event> event;
	PartitionLocalMergeState local_state;
	PartitionGlobalMergeStates &hash_groups;
};

TaskExecutionResult PartitionMergeTask::ExecuteTask(TaskExecutionMode mode) {
	// Loop until all hash groups are done
	size_t sorted = 0;
	while (sorted < hash_groups.states.size()) {
		// First check if there is an unfinished task for this thread
		if (executor.HasError()) {
			return TaskExecutionResult::TASK_ERROR;
		}
		if (!local_state.TaskFinished()) {
			local_state.ExecuteTask();
			continue;
		}

		// Thread is done with its assigned task, try to fetch new work
		for (auto group = sorted; group < hash_groups.states.size(); ++group) {
			auto &global_state = hash_groups.states[group];
			if (global_state->IsSorted()) {
				// This hash group is done
				// Update the high water mark of densely completed groups
				if (sorted == group) {
					++sorted;
				}
				continue;
			}

			// Try to assign work for this hash group to this thread
			if (global_state->AssignTask(local_state)) {
				// We assigned a task to this thread!
				// Break out of this loop to re-enter the top-level loop and execute the task
				break;
			}

			// Hash group global state couldn't assign a task to this thread
			// Try to prepare the next stage
			if (!global_state->TryPrepareNextStage()) {
				// This current hash group is not yet done
				// But we were not able to assign a task for it to this thread
				// See if the next hash group is better
				continue;
			}

			// We were able to prepare the next stage for this hash group!
			// Try to assign a task once more
			if (global_state->AssignTask(local_state)) {
				// We assigned a task to this thread!
				// Break out of this loop to re-enter the top-level loop and execute the task
				break;
			}

			// We were able to prepare the next merge round,
			// but we were not able to assign a task for it to this thread
			// The tasks were assigned to other threads while this thread waited for the lock
			// Go to the next iteration to see if another hash group has a task
		}
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void PartitionMergeEvent::Schedule() {
	auto &context = pipeline->GetClientContext();

	// Schedule tasks equal to the number of threads, which will each merge multiple partitions
	auto &ts = TaskScheduler::GetScheduler(context);
	idx_t num_threads = ts.NumberOfThreads();

	vector<unique_ptr<Task>> merge_tasks;
	for (idx_t tnum = 0; tnum < num_threads; tnum++) {
		merge_tasks.emplace_back(make_uniq<PartitionMergeTask>(shared_from_this(), context, merge_states));
	}
	SetTasks(std::move(merge_tasks));
}

PartitionLocalSourceState::PartitionLocalSourceState(PartitionGlobalSinkState &gstate_p) : gstate(gstate_p) {
	const auto &input_types = gstate.payload_types;
	layout.Initialize(input_types);
	input_chunk.Initialize(gstate.allocator, input_types);
}

void PartitionLocalSourceState::MaterializeSortedData() {
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
		heap = make_uniq<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	heap->count = std::accumulate(heap->blocks.begin(), heap->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
}

idx_t PartitionLocalSourceState::GeneratePartition(const idx_t hash_bin_p) {
	//	Get rid of any stale data
	hash_bin = hash_bin_p;

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	idx_t count = 0;
	if (hash_bin < gstate.hash_groups.size() && gstate.hash_groups[hash_bin]) {
		count = gstate.hash_groups[hash_bin]->count;
	} else if (gstate.rows && !hash_bin) {
		count = gstate.count;
	} else {
		return count;
	}

	//	Initialise masks to false
	const auto bit_count = ValidityMask::ValidityMaskSize(count);
	partition_bits.clear();
	partition_bits.resize(bit_count, 0);
	partition_mask.Initialize(partition_bits.data());

	order_bits.clear();
	order_bits.resize(bit_count, 0);
	order_mask.Initialize(order_bits.data());

	// Scan the sorted data into new Collections
	auto external = gstate.external;
	if (gstate.rows && !hash_bin) {
		// Simple mask
		partition_mask.SetValidUnsafe(0);
		order_mask.SetValidUnsafe(0);
		//	No partition - align the heap blocks with the row blocks
		rows = gstate.rows->CloneEmpty(gstate.rows->keep_pinned);
		heap = gstate.strings->CloneEmpty(gstate.strings->keep_pinned);
		RowDataCollectionScanner::AlignHeapBlocks(*rows, *heap, *gstate.rows, *gstate.strings, layout);
		external = true;
	} else if (hash_bin < gstate.hash_groups.size() && gstate.hash_groups[hash_bin]) {
		// Overwrite the collections with the sorted data
		hash_group = std::move(gstate.hash_groups[hash_bin]);
		hash_group->ComputeMasks(partition_mask, order_mask);
		MaterializeSortedData();
	} else {
		return count;
	}

	scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, external, false);

	return count;
}

} // namespace duckdb
