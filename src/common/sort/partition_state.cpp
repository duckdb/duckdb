#include "duckdb/common/sort/partition_state.hpp"

#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/executor_task.hpp"

#include <numeric>

namespace duckdb {

PartitionGlobalHashGroup::PartitionGlobalHashGroup(BufferManager &buffer_manager, const Orders &partitions,
                                                   const Orders &orders, const Types &payload_types, bool external)
    : count(0) {

	RowLayout payload_layout;
	payload_layout.Initialize(payload_types);
	global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
	global_sort->external = external;

	//	Set up a comparator for the partition subset
	partition_layout = global_sort->sort_layout.GetPrefixComparisonLayout(partitions.size());
}

void PartitionGlobalHashGroup::ComputeMasks(ValidityMask &partition_mask, OrderMasks &order_masks) {
	D_ASSERT(count > 0);

	SBIterator prev(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator curr(*global_sort, ExpressionType::COMPARE_LESSTHAN);

	partition_mask.SetValidUnsafe(0);
	unordered_map<idx_t, SortLayout> prefixes;
	for (auto &order_mask : order_masks) {
		order_mask.second.SetValidUnsafe(0);
		D_ASSERT(order_mask.first >= partition_layout.column_count);
		prefixes[order_mask.first] = global_sort->sort_layout.GetPrefixComparisonLayout(order_mask.first);
	}

	for (++curr; curr.GetIndex() < count; ++curr) {
		//	Compare the partition subset first because if that differs, then so does the full ordering
		const auto part_cmp = ComparePartitions(prev, curr);

		if (part_cmp) {
			partition_mask.SetValidUnsafe(curr.GetIndex());
			for (auto &order_mask : order_masks) {
				order_mask.second.SetValidUnsafe(curr.GetIndex());
			}
		} else {
			for (auto &order_mask : order_masks) {
				if (prev.Compare(curr, prefixes[order_mask.first])) {
					order_mask.second.SetValidUnsafe(curr.GetIndex());
				}
			}
		}
		++prev;
	}
}

void PartitionGlobalSinkState::GenerateOrderings(Orders &partitions, Orders &orders,
                                                 const vector<unique_ptr<Expression>> &partition_bys,
                                                 const Orders &order_bys,
                                                 const vector<unique_ptr<BaseStatistics>> &partition_stats) {

	// we sort by both 1) partition by expression list and 2) order by expressions
	const auto partition_cols = partition_bys.size();
	for (idx_t prt_idx = 0; prt_idx < partition_cols; prt_idx++) {
		auto &pexpr = partition_bys[prt_idx];

		if (partition_stats.empty() || !partition_stats[prt_idx]) {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(), nullptr);
		} else {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(),
			                    partition_stats[prt_idx]->ToUnique());
		}
		partitions.emplace_back(orders.back().Copy());
	}

	for (const auto &order : order_bys) {
		orders.emplace_back(order.Copy());
	}
}

PartitionGlobalSinkState::PartitionGlobalSinkState(ClientContext &context,
                                                   const vector<unique_ptr<Expression>> &partition_bys,
                                                   const vector<BoundOrderByNode> &order_bys,
                                                   const Types &payload_types,
                                                   const vector<unique_ptr<BaseStatistics>> &partition_stats,
                                                   idx_t estimated_cardinality)
    : context(context), buffer_manager(BufferManager::GetBufferManager(context)), allocator(Allocator::Get(context)),
      fixed_bits(0), payload_types(payload_types), memory_per_thread(0), max_bits(1), count(0) {

	GenerateOrderings(partitions, orders, partition_bys, order_bys, partition_stats);

	memory_per_thread = PhysicalOperator::GetMaxThreadMemory(context);
	external = ClientConfig::GetConfig(context).force_external;

	const auto thread_pages = PreviousPowerOfTwo(memory_per_thread / (4 * buffer_manager.GetBlockAllocSize()));
	while (max_bits < 10 && (thread_pages >> max_bits) > 1) {
		++max_bits;
	}

	if (!orders.empty()) {
		if (partitions.empty()) {
			//	Sort early into a dedicated hash group if we only sort.
			grouping_types.Initialize(payload_types);
			auto new_group =
			    make_uniq<PartitionGlobalHashGroup>(buffer_manager, partitions, orders, payload_types, external);
			hash_groups.emplace_back(std::move(new_group));
		} else {
			auto types = payload_types;
			types.push_back(LogicalType::HASH);
			grouping_types.Initialize(types);
			ResizeGroupingData(estimated_cardinality);
		}
	}
}

bool PartitionGlobalSinkState::HasMergeTasks() const {
	if (grouping_data) {
		auto &groups = grouping_data->GetPartitions();
		return !groups.empty();
	} else if (!hash_groups.empty()) {
		D_ASSERT(hash_groups.size() == 1);
		return hash_groups[0]->count > 0;
	} else {
		return false;
	}
}

void PartitionGlobalSinkState::SyncPartitioning(const PartitionGlobalSinkState &other) {
	fixed_bits = other.grouping_data ? other.grouping_data->GetRadixBits() : 0;

	const auto old_bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	if (fixed_bits != old_bits) {
		const auto hash_col_idx = payload_types.size();
		grouping_data = make_uniq<RadixPartitionedTupleData>(buffer_manager, grouping_types, fixed_bits, hash_col_idx);
	}
}

unique_ptr<RadixPartitionedTupleData> PartitionGlobalSinkState::CreatePartition(idx_t new_bits) const {
	const auto hash_col_idx = payload_types.size();
	return make_uniq<RadixPartitionedTupleData>(buffer_manager, grouping_types, new_bits, hash_col_idx);
}

void PartitionGlobalSinkState::ResizeGroupingData(idx_t cardinality) {
	//	Have we started to combine? Then just live with it.
	if (fixed_bits || (grouping_data && !grouping_data->GetPartitions().empty())) {
		return;
	}
	//	Is the average partition size too large?
	const idx_t partition_size = STANDARD_ROW_GROUPS_SIZE;
	const auto bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	auto new_bits = bits ? bits : 4;
	while (new_bits < max_bits && (cardinality / RadixPartitioning::NumberOfPartitions(new_bits)) > partition_size) {
		++new_bits;
	}

	// Repartition the grouping data
	if (new_bits != bits) {
		grouping_data = CreatePartition(new_bits);
	}
}

void PartitionGlobalSinkState::SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// We are done if the local_partition is right sized.
	auto &local_radix = local_partition->Cast<RadixPartitionedTupleData>();
	const auto new_bits = grouping_data->GetRadixBits();
	if (local_radix.GetRadixBits() == new_bits) {
		return;
	}

	// If the local partition is now too small, flush it and reallocate
	auto new_partition = CreatePartition(new_bits);
	local_partition->FlushAppendState(*local_append);
	local_partition->Repartition(*new_partition);

	local_partition = std::move(new_partition);
	local_append = make_uniq<PartitionedTupleDataAppendState>();
	local_partition->InitializeAppendState(*local_append);
}

void PartitionGlobalSinkState::UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// Make sure grouping_data doesn't change under us.
	lock_guard<mutex> guard(lock);

	if (!local_partition) {
		local_partition = CreatePartition(grouping_data->GetRadixBits());
		local_append = make_uniq<PartitionedTupleDataAppendState>();
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

PartitionLocalMergeState::PartitionLocalMergeState(PartitionGlobalSinkState &gstate)
    : merge_state(nullptr), stage(PartitionSortStage::INIT), finished(true), executor(gstate.context) {

	//	 Set up the sort expression computation.
	vector<LogicalType> sort_types;
	for (auto &order : gstate.orders) {
		auto &oexpr = order.expression;
		sort_types.emplace_back(oexpr->return_type);
		executor.AddExpression(*oexpr);
	}
	sort_chunk.Initialize(gstate.allocator, sort_types);
	payload_chunk.Initialize(gstate.allocator, gstate.payload_types);
}

void PartitionLocalMergeState::Scan() {
	if (!merge_state->group_data) {
		//	OVER(ORDER BY...)
		//	Already sorted
		return;
	}

	auto &group_data = *merge_state->group_data;
	auto &hash_group = *merge_state->hash_group;
	auto &chunk_state = merge_state->chunk_state;
	// Copy the data from the group into the sort code.
	auto &global_sort = *hash_group.global_sort;
	LocalSortState local_sort;
	local_sort.Initialize(global_sort, global_sort.buffer_manager);

	TupleDataScanState local_scan;
	group_data.InitializeScan(local_scan, merge_state->column_ids);
	while (group_data.Scan(chunk_state, local_scan, payload_chunk)) {
		sort_chunk.Reset();
		executor.Execute(payload_chunk, sort_chunk);

		local_sort.SinkChunk(sort_chunk, payload_chunk);
		if (local_sort.SizeInBytes() > merge_state->memory_per_thread) {
			local_sort.Sort(global_sort, true);
		}
		hash_group.count += payload_chunk.size();
	}

	global_sort.AddLocalState(local_sort);
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
		auto payload_types = gstate.payload_types;
		if (!group_types.empty()) {
			// OVER(PARTITION BY...)
			group_chunk.Initialize(allocator, group_types);
			payload_types.emplace_back(LogicalType::HASH);
		} else {
			// OVER(ORDER BY...)
			for (idx_t ord_idx = 0; ord_idx < gstate.orders.size(); ord_idx++) {
				auto &pexpr = *gstate.orders[ord_idx].expression.get();
				group_types.push_back(pexpr.return_type);
				executor.AddExpression(pexpr);
			}
			group_chunk.Initialize(allocator, group_types);

			//	Single partition
			auto &global_sort = *gstate.hash_groups[0]->global_sort;
			local_sort = make_uniq<LocalSortState>();
			local_sort->Initialize(global_sort, global_sort.buffer_manager);
		}
		// OVER(...)
		payload_chunk.Initialize(allocator, payload_types);
	} else {
		// OVER()
		payload_layout.Initialize(gstate.payload_types);
	}
}

void PartitionLocalSinkState::Hash(DataChunk &input_chunk, Vector &hash_vector) {
	const auto count = input_chunk.size();
	D_ASSERT(group_chunk.ColumnCount() > 0);

	// OVER(PARTITION BY...) (hash grouping)
	group_chunk.Reset();
	executor.Execute(input_chunk, group_chunk);
	VectorOperations::Hash(group_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < group_chunk.ColumnCount(); ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, group_chunk.data[prt_idx], count);
	}
}

void PartitionLocalSinkState::Sink(DataChunk &input_chunk) {
	gstate.count += input_chunk.size();

	// OVER()
	if (sort_cols == 0) {
		//	No sorts, so build paged row chunks
		if (!rows) {
			const auto entry_size = payload_layout.GetRowWidth();
			const auto block_size = gstate.buffer_manager.GetBlockSize();
			const auto capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, block_size / entry_size + 1);
			rows = make_uniq<RowDataCollection>(gstate.buffer_manager, capacity, entry_size);
			strings = make_uniq<RowDataCollection>(gstate.buffer_manager, block_size, 1U, true);
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

	if (local_sort) {
		//	OVER(ORDER BY...)
		group_chunk.Reset();
		executor.Execute(input_chunk, group_chunk);
		local_sort->SinkChunk(group_chunk, input_chunk);

		auto &hash_group = *gstate.hash_groups[0];
		hash_group.count += input_chunk.size();

		if (local_sort->SizeInBytes() > gstate.memory_per_thread) {
			auto &global_sort = *hash_group.global_sort;
			local_sort->Sort(global_sort, true);
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

	if (local_sort) {
		//	OVER(ORDER BY...)
		auto &hash_group = *gstate.hash_groups[0];
		auto &global_sort = *hash_group.global_sort;
		global_sort.AddLocalState(*local_sort);
		local_sort.reset();
		return;
	}

	// OVER(...)
	gstate.CombineLocalPartition(local_partition, local_append);
}

PartitionGlobalMergeState::PartitionGlobalMergeState(PartitionGlobalSinkState &sink, GroupDataPtr group_data_p,
                                                     hash_t hash_bin)
    : sink(sink), group_data(std::move(group_data_p)), group_idx(sink.hash_groups.size()),
      memory_per_thread(sink.memory_per_thread),
      num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(sink.context).NumberOfThreads())),
      stage(PartitionSortStage::INIT), total_tasks(0), tasks_assigned(0), tasks_completed(0) {

	auto new_group = make_uniq<PartitionGlobalHashGroup>(sink.buffer_manager, sink.partitions, sink.orders,
	                                                     sink.payload_types, sink.external);
	sink.hash_groups.emplace_back(std::move(new_group));

	hash_group = sink.hash_groups[group_idx].get();
	global_sort = sink.hash_groups[group_idx]->global_sort.get();

	sink.bin_groups[hash_bin] = group_idx;

	column_ids.reserve(sink.payload_types.size());
	for (column_t i = 0; i < sink.payload_types.size(); ++i) {
		column_ids.emplace_back(i);
	}
	group_data->InitializeScan(chunk_state, column_ids);
}

PartitionGlobalMergeState::PartitionGlobalMergeState(PartitionGlobalSinkState &sink)
    : sink(sink), group_idx(0), memory_per_thread(sink.memory_per_thread),
      num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(sink.context).NumberOfThreads())),
      stage(PartitionSortStage::INIT), total_tasks(0), tasks_assigned(0), tasks_completed(0) {

	const hash_t hash_bin = 0;
	hash_group = sink.hash_groups[group_idx].get();
	global_sort = sink.hash_groups[group_idx]->global_sort.get();

	sink.bin_groups[hash_bin] = group_idx;
}

void PartitionLocalMergeState::Prepare() {
	merge_state->group_data.reset();

	auto &global_sort = *merge_state->global_sort;
	global_sort.PrepareMergePhase();
}

void PartitionLocalMergeState::Merge() {
	auto &global_sort = *merge_state->global_sort;
	MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
	merge_sorter.PerformInMergeRound();
}

void PartitionLocalMergeState::Sorted() {
	merge_state->sink.OnSortedPartition(merge_state->group_idx);
}

void PartitionLocalMergeState::ExecuteTask() {
	switch (stage) {
	case PartitionSortStage::SCAN:
		Scan();
		break;
	case PartitionSortStage::PREPARE:
		Prepare();
		break;
	case PartitionSortStage::MERGE:
		Merge();
		break;
	case PartitionSortStage::SORTED:
		Sorted();
		break;
	default:
		throw InternalException("Unexpected PartitionSortStage in ExecuteTask!");
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
		//	If the partitions are unordered, don't scan in parallel
		//	because it produces non-deterministic orderings.
		//	This can theoretically happen with ORDER BY,
		//	but that is something the query should be explicit about.
		total_tasks = sink.orders.size() > sink.partitions.size() ? num_threads : 1;
		stage = PartitionSortStage::SCAN;
		return true;

	case PartitionSortStage::SCAN:
		total_tasks = 1;
		stage = PartitionSortStage::PREPARE;
		return true;

	case PartitionSortStage::PREPARE:
		if (!(global_sort->sorted_blocks.size() / 2)) {
			break;
		}
		stage = PartitionSortStage::MERGE;
		global_sort->InitializeMergeRound();
		total_tasks = num_threads;
		return true;

	case PartitionSortStage::MERGE:
		global_sort->CompleteMergeRound(true);
		if (!(global_sort->sorted_blocks.size() / 2)) {
			break;
		}
		global_sort->InitializeMergeRound();
		total_tasks = num_threads;
		return true;

	case PartitionSortStage::SORTED:
		stage = PartitionSortStage::FINISHED;
		total_tasks = 0;
		return false;

	case PartitionSortStage::FINISHED:
		return false;
	}

	stage = PartitionSortStage::SORTED;
	total_tasks = 1;

	return true;
}

PartitionGlobalMergeStates::PartitionGlobalMergeStates(PartitionGlobalSinkState &sink) {
	// Schedule all the sorts for maximum thread utilisation
	if (sink.grouping_data) {
		auto &partitions = sink.grouping_data->GetPartitions();
		sink.bin_groups.resize(partitions.size(), partitions.size());
		for (hash_t hash_bin = 0; hash_bin < partitions.size(); ++hash_bin) {
			auto &group_data = partitions[hash_bin];
			// Prepare for merge sort phase
			if (group_data->Count()) {
				auto state = make_uniq<PartitionGlobalMergeState>(sink, std::move(group_data), hash_bin);
				states.emplace_back(std::move(state));
			}
		}
	} else {
		//	OVER(ORDER BY...)
		//	Already sunk into the single global sort, so set up single merge with no data
		sink.bin_groups.resize(1, 1);
		auto state = make_uniq<PartitionGlobalMergeState>(sink);
		states.emplace_back(std::move(state));
	}

	sink.OnBeginMerge();
}

class PartitionMergeTask : public ExecutorTask {
public:
	PartitionMergeTask(shared_ptr<Event> event_p, ClientContext &context_p, PartitionGlobalMergeStates &hash_groups_p,
	                   PartitionGlobalSinkState &gstate, const PhysicalOperator &op)
	    : ExecutorTask(context_p, std::move(event_p), op), local_state(gstate), hash_groups(hash_groups_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	struct ExecutorCallback : public PartitionGlobalMergeStates::Callback {
		explicit ExecutorCallback(Executor &executor) : executor(executor) {
		}

		bool HasError() const override {
			return executor.HasError();
		}

		Executor &executor;
	};

	PartitionLocalMergeState local_state;
	PartitionGlobalMergeStates &hash_groups;
};

bool PartitionGlobalMergeStates::ExecuteTask(PartitionLocalMergeState &local_state, Callback &callback) {
	// Loop until all hash groups are done
	size_t sorted = 0;
	while (sorted < states.size()) {
		// First check if there is an unfinished task for this thread
		if (callback.HasError()) {
			return false;
		}
		if (!local_state.TaskFinished()) {
			local_state.ExecuteTask();
			continue;
		}

		// Thread is done with its assigned task, try to fetch new work
		for (auto group = sorted; group < states.size(); ++group) {
			auto &global_state = states[group];
			if (global_state->IsFinished()) {
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

	return true;
}

TaskExecutionResult PartitionMergeTask::ExecuteTask(TaskExecutionMode mode) {
	ExecutorCallback callback(executor);

	if (!hash_groups.ExecuteTask(local_state, callback)) {
		return TaskExecutionResult::TASK_ERROR;
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void PartitionMergeEvent::Schedule() {
	auto &context = pipeline->GetClientContext();

	// Schedule tasks equal to the number of threads, which will each merge multiple partitions
	auto &ts = TaskScheduler::GetScheduler(context);
	auto num_threads = NumericCast<idx_t>(ts.NumberOfThreads());

	vector<shared_ptr<Task>> merge_tasks;
	for (idx_t tnum = 0; tnum < num_threads; tnum++) {
		merge_tasks.emplace_back(make_uniq<PartitionMergeTask>(shared_from_this(), context, merge_states, gstate, op));
	}
	SetTasks(std::move(merge_tasks));
}

} // namespace duckdb
