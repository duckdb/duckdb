#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// HashedSortGroup
//===--------------------------------------------------------------------===//
HashedSortGroup::HashedSortGroup(ClientContext &context, const Orders &orders, const Types &input_types,
                                 idx_t group_idx)
    : group_idx(group_idx), tasks_completed(0) {
	vector<idx_t> projection_map;
	sort = make_uniq<Sort>(context, orders, input_types, projection_map);
	sort_global = sort->GetGlobalSinkState(context);
}

//===--------------------------------------------------------------------===//
// HashedSortGlobalSinkState
//===--------------------------------------------------------------------===//
void HashedSortGlobalSinkState::GenerateOrderings(Orders &partitions, Orders &orders,
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

HashedSortGlobalSinkState::HashedSortGlobalSinkState(ClientContext &context,
                                                     const vector<unique_ptr<Expression>> &partition_bys,
                                                     const vector<BoundOrderByNode> &order_bys,
                                                     const Types &input_types,
                                                     const vector<unique_ptr<BaseStatistics>> &partition_stats,
                                                     idx_t estimated_cardinality)
    : context(context), buffer_manager(BufferManager::GetBufferManager(context)), allocator(Allocator::Get(context)),
      fixed_bits(0), payload_types(input_types), max_bits(1), count(0) {

	GenerateOrderings(partitions, orders, partition_bys, order_bys, partition_stats);

	// The payload prefix is the same as the input schema
	for (column_t i = 0; i < payload_types.size(); ++i) {
		scan_ids.emplace_back(i);
	}

	//	We have to compute ordering expressions ourselves and materialise them.
	//	To do this, we scan the orders and add generate extra payload columns that we can reference.
	for (auto &order : orders) {
		auto &expr = *order.expression;
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = expr.Cast<BoundReferenceExpression>();
			sort_ids.emplace_back(ref.index);
			continue;
		}

		//	Real expression - replace with a ref and save the expression
		auto saved = std::move(order.expression);
		const auto type = saved->return_type;
		const auto idx = payload_types.size();
		order.expression = make_uniq<BoundReferenceExpression>(type, idx);
		sort_ids.emplace_back(idx);
		payload_types.emplace_back(type);
		sort_exprs.emplace_back(std::move(saved));
	}

	const auto memory_per_thread = PhysicalOperator::GetMaxThreadMemory(context);
	const auto thread_pages = PreviousPowerOfTwo(memory_per_thread / (4 * buffer_manager.GetBlockAllocSize()));
	while (max_bits < 8 && (thread_pages >> max_bits) > 1) {
		++max_bits;
	}

	grouping_types_ptr = make_shared_ptr<TupleDataLayout>();
	if (!orders.empty()) {
		if (partitions.empty()) {
			//	Sort early into a dedicated hash group if we only sort.
			grouping_types_ptr->Initialize(payload_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			auto new_group = make_uniq<HashedSortGroup>(context, orders, payload_types, idx_t(0));
			hash_groups.emplace_back(std::move(new_group));
		} else {
			auto types = payload_types;
			types.push_back(LogicalType::HASH);
			grouping_types_ptr->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			Rehash(estimated_cardinality);
		}
	}
}

unique_ptr<RadixPartitionedTupleData> HashedSortGlobalSinkState::CreatePartition(idx_t new_bits) const {
	const auto hash_col_idx = payload_types.size();
	return make_uniq<RadixPartitionedTupleData>(buffer_manager, grouping_types_ptr, new_bits, hash_col_idx);
}

void HashedSortGlobalSinkState::Rehash(idx_t cardinality) {
	//	Have we started to combine? Then just live with it.
	if (fixed_bits) {
		return;
	}
	//	Is the average partition size too large?
	const idx_t partition_size = DEFAULT_ROW_GROUP_SIZE;
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

void HashedSortGlobalSinkState::SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// We are done if the local_partition is right sized.
	const auto new_bits = grouping_data->GetRadixBits();
	if (local_partition->GetRadixBits() == new_bits) {
		return;
	}

	// If the local partition is now too small, flush it and reallocate
	auto new_partition = CreatePartition(new_bits);
	local_partition->FlushAppendState(*local_append);
	local_partition->Repartition(context, *new_partition);

	local_partition = std::move(new_partition);
	local_append = make_uniq<PartitionedTupleDataAppendState>();
	local_partition->InitializeAppendState(*local_append);
}

void HashedSortGlobalSinkState::UpdateLocalPartition(GroupingPartition &local_partition,
                                                     GroupingAppend &partition_append) {
	// Make sure grouping_data doesn't change under us.
	lock_guard<mutex> guard(lock);

	if (!local_partition) {
		local_partition = CreatePartition(grouping_data->GetRadixBits());
		partition_append = make_uniq<PartitionedTupleDataAppendState>();
		local_partition->InitializeAppendState(*partition_append);
		return;
	}

	// 	Grow the groups if they are too big
	Rehash(count);

	//	Sync local partition to have the same bit count
	SyncLocalPartition(local_partition, partition_append);
}

void HashedSortGlobalSinkState::CombineLocalPartition(GroupingPartition &local_partition,
                                                      GroupingAppend &local_append) {
	if (!local_partition) {
		return;
	}
	local_partition->FlushAppendState(*local_append);

	// Make sure grouping_data doesn't change under us.
	// Combine has an internal mutex, so this is single-threaded anyway.
	lock_guard<mutex> guard(lock);
	SyncLocalPartition(local_partition, local_append);
	fixed_bits = true;

	//	We now know the number of hash_groups (some may be empty)
	auto &groups = local_partition->GetPartitions();
	if (hash_groups.empty()) {
		hash_groups.resize(groups.size());
	}

	//	Create missing HashedSortGroups inside the mutex
	for (idx_t group_idx = 0; group_idx < groups.size(); ++group_idx) {
		auto &hash_group = hash_groups[group_idx];
		if (hash_group) {
			continue;
		}

		auto &group_data = groups[group_idx];
		if (group_data->Count()) {
			hash_group = make_uniq<HashedSortGroup>(context, orders, payload_types, group_idx);
		}
	}
}

void HashedSortGlobalSinkState::Finalize(ClientContext &context, InterruptState &interrupt_state) {
	// OVER()
	if (unsorted) {
		return;
	}

	// OVER(...)
	D_ASSERT(!hash_groups.empty());
	for (auto &hash_group : hash_groups) {
		if (!hash_group) {
			continue;
		}
		OperatorSinkFinalizeInput finalize {*hash_group->sort_global, interrupt_state};
		hash_group->sort->Finalize(context, finalize);
	}
}

bool HashedSortGlobalSinkState::HasMergeTasks() const {
	return (!hash_groups.empty());
}

//===--------------------------------------------------------------------===//
// HashedSortLocalSinkState
//===--------------------------------------------------------------------===//
HashedSortLocalSinkState::HashedSortLocalSinkState(ExecutionContext &context, HashedSortGlobalSinkState &gstate)
    : gstate(gstate), allocator(Allocator::Get(context.client)), hash_exec(context.client), sort_exec(context.client) {

	vector<LogicalType> group_types;
	for (idx_t prt_idx = 0; prt_idx < gstate.partitions.size(); prt_idx++) {
		auto &pexpr = *gstate.partitions[prt_idx].expression.get();
		group_types.push_back(pexpr.return_type);
		hash_exec.AddExpression(pexpr);
	}
	sort_col_count = gstate.orders.size() + group_types.size();

	vector<LogicalType> sort_types;
	for (const auto &expr : gstate.sort_exprs) {
		sort_types.emplace_back(expr->return_type);
		sort_exec.AddExpression(*expr);
	}
	sort_chunk.Initialize(context.client, sort_types);

	if (sort_col_count) {
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
				hash_exec.AddExpression(pexpr);
			}
			group_chunk.Initialize(allocator, group_types);

			//	Single partition
			auto &sort = *gstate.hash_groups[0]->sort;
			sort_local = sort.GetLocalSinkState(context);
		}
		// OVER(...)
		payload_chunk.Initialize(allocator, payload_types);
	}
}

void HashedSortLocalSinkState::Hash(DataChunk &input_chunk, Vector &hash_vector) {
	const auto count = input_chunk.size();
	D_ASSERT(group_chunk.ColumnCount() > 0);

	// OVER(PARTITION BY...) (hash grouping)
	group_chunk.Reset();
	hash_exec.Execute(input_chunk, group_chunk);
	VectorOperations::Hash(group_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < group_chunk.ColumnCount(); ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, group_chunk.data[prt_idx], count);
	}
}

void HashedSortLocalSinkState::Sink(ExecutionContext &context, DataChunk &input_chunk) {
	gstate.count += input_chunk.size();

	// Window::Sink:
	// PartitionedTupleData::Append
	// Sort::Sink
	// ColumnDataCollection::Append

	// OVER()
	if (sort_col_count == 0) {
		if (!unsorted) {
			unsorted = make_uniq<ColumnDataCollection>(context.client, gstate.payload_types);
			unsorted->InitializeAppend(unsorted_append);
		}
		unsorted->Append(unsorted_append, input_chunk);
		return;
	}

	//	Payload prefix is the input data
	payload_chunk.Reset();
	for (column_t i = 0; i < input_chunk.ColumnCount(); ++i) {
		payload_chunk.data[i].Reference(input_chunk.data[i]);
	}

	//	Compute any sort columns that are not references and append them to the end of the payload
	if (!gstate.sort_exprs.empty()) {
		sort_chunk.Reset();
		sort_exec.Execute(input_chunk, sort_chunk);
		for (column_t i = 0; i < sort_chunk.ColumnCount(); ++i) {
			payload_chunk.data[input_chunk.ColumnCount() + i].Reference(sort_chunk.data[i]);
		}
	}
	payload_chunk.SetCardinality(input_chunk);

	//	OVER(ORDER BY...)
	if (sort_local) {
		auto &hash_group = *gstate.hash_groups[0];
		OperatorSinkInput input {*hash_group.sort_global, *sort_local, interrupt};
		hash_group.sort->Sink(context, payload_chunk, input);
		return;
	}

	// OVER(PARTITION BY...)
	auto &hash_vector = payload_chunk.data.back();
	Hash(input_chunk, hash_vector);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}

	gstate.UpdateLocalPartition(local_grouping, grouping_append);
	local_grouping->Append(*grouping_append, payload_chunk);
}

void HashedSortLocalSinkState::Combine(ExecutionContext &context) {
	// Window::Combine:
	// Sort::Sink then Sort::Combine (per hash partition)
	// Sort::Combine
	// ColumnDataCollection::Combine

	// OVER()
	if (sort_col_count == 0) {
		// Only one partition again, so need a global lock.
		lock_guard<mutex> glock(gstate.lock);
		if (gstate.unsorted) {
			if (unsorted) {
				gstate.unsorted->Combine(*unsorted);
				unsorted.reset();
			}
		} else {
			gstate.unsorted = std::move(unsorted);
		}
		return;
	}

	//	OVER(ORDER BY...)
	if (sort_local) {
		auto &hash_group = *gstate.hash_groups[0];
		auto &sort = *hash_group.sort;
		OperatorSinkCombineInput input {*hash_group.sort_global, *sort_local, interrupt};
		sort.Combine(context, input);
		sort_local.reset();
		return;
	}

	// OVER(PARTITION BY...)
	if (!local_grouping) {
		return;
	}

	// Flush our data and lock the bit count
	gstate.CombineLocalPartition(local_grouping, grouping_append);

	//	Don't scan the hash column
	vector<column_t> column_ids;
	for (column_t i = 0; i < gstate.payload_types.size(); ++i) {
		column_ids.emplace_back(i);
	}

	//	Loop over the partitions and add them to each hash group's global sort state
	TupleDataScanState scan_state;
	DataChunk chunk;
	auto &partitions = local_grouping->GetPartitions();
	for (hash_t hash_bin = 0; hash_bin < partitions.size(); ++hash_bin) {
		auto &partition = *partitions[hash_bin];
		if (!partition.Count()) {
			continue;
		}

		partition.InitializeScan(scan_state, column_ids, TupleDataPinProperties::DESTROY_AFTER_DONE);
		if (chunk.data.empty()) {
			partition.InitializeScanChunk(scan_state, chunk);
		}

		auto &hash_group = *gstate.hash_groups[hash_bin];
		auto &sort = *hash_group.sort;
		sort_local = sort.GetLocalSinkState(context);
		OperatorSinkInput sink {*hash_group.sort_global, *sort_local, interrupt};
		while (partition.Scan(scan_state, chunk)) {
			sort.Sink(context, chunk, sink);
		}

		OperatorSinkCombineInput combine {*hash_group.sort_global, *sort_local, interrupt};
		sort.Combine(context, combine);
	}
}

//===--------------------------------------------------------------------===//
// HashedSortMaterializeTask
//===--------------------------------------------------------------------===//
class HashedSortMaterializeTask : public ExecutorTask {
public:
	HashedSortMaterializeTask(Pipeline &pipeline, shared_ptr<Event> event, const PhysicalOperator &op,
	                          HashedSortGroup &hash_group, idx_t tasks_scheduled,
	                          optional_ptr<HashedSortCallback> callback);

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	string TaskType() const override {
		return "HashedSortMaterializeTask";
	}

private:
	Pipeline &pipeline;
	HashedSortGroup &hash_group;
	const idx_t tasks_scheduled;
	optional_ptr<HashedSortCallback> callback;
};

HashedSortMaterializeTask::HashedSortMaterializeTask(Pipeline &pipeline, shared_ptr<Event> event,
                                                     const PhysicalOperator &op, HashedSortGroup &hash_group,
                                                     idx_t tasks_scheduled, optional_ptr<HashedSortCallback> callback)
    : ExecutorTask(pipeline.GetClientContext(), std::move(event), op), pipeline(pipeline), hash_group(hash_group),
      tasks_scheduled(tasks_scheduled), callback(callback) {
}

TaskExecutionResult HashedSortMaterializeTask::ExecuteTask(TaskExecutionMode mode) {
	ExecutionContext execution(pipeline.GetClientContext(), *thread_context, &pipeline);
	auto &sort = *hash_group.sort;
	auto &sort_global = *hash_group.sort_source;
	auto sort_local = sort.GetLocalSourceState(execution, sort_global);
	InterruptState interrupt((weak_ptr<Task>(shared_from_this())));
	OperatorSourceInput input {sort_global, *sort_local, interrupt};
	sort.MaterializeColumnData(execution, input);
	if (++hash_group.tasks_completed == tasks_scheduled && callback) {
		hash_group.sorted = sort.GetColumnData(input);
		callback->OnSortedGroup(hash_group);
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

//===--------------------------------------------------------------------===//
// HashedSortMaterializeEvent
//===--------------------------------------------------------------------===//
HashedSortMaterializeEvent::HashedSortMaterializeEvent(HashedSortGlobalSinkState &gstate, Pipeline &pipeline,
                                                       const PhysicalOperator &op, HashedSortCallback *callback)
    : BasePipelineEvent(pipeline), gstate(gstate), op(op), callback(callback) {
}

void HashedSortMaterializeEvent::Schedule() {
	auto &client = pipeline->GetClientContext();

	// Schedule as many tasks per hash group as the sort will allow
	auto &ts = TaskScheduler::GetScheduler(client);
	const auto num_threads = NumericCast<idx_t>(ts.NumberOfThreads());

	vector<shared_ptr<Task>> merge_tasks;
	for (auto &hash_group : gstate.hash_groups) {
		if (!hash_group) {
			continue;
		}
		auto &sort = *hash_group->sort;
		auto &global_sink = *hash_group->sort_global;
		hash_group->sort_source = sort.GetGlobalSourceState(client, global_sink);
		const auto tasks_scheduled = MinValue<idx_t>(num_threads, hash_group->sort_source->MaxThreads());
		for (idx_t t = 0; t < tasks_scheduled; ++t) {
			merge_tasks.emplace_back(make_uniq<HashedSortMaterializeTask>(*pipeline, shared_from_this(), op,
			                                                              *hash_group, tasks_scheduled, callback));
		}
	}

	SetTasks(std::move(merge_tasks));
}

} // namespace duckdb
