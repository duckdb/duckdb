#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// HashedSortGroup
//===--------------------------------------------------------------------===//
// Formerly PartitionGlobalHashGroup
class HashedSortGroup {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	HashedSortGroup(ClientContext &client, optional_ptr<Sort> sort, idx_t group_idx);

	const idx_t group_idx;
	atomic<idx_t> count;

	//	Sink
	optional_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> sort_global;

	//	Source
	mutex scan_lock;
	TupleDataParallelScanState parallel_scan;
	atomic<idx_t> tasks_completed;
	unique_ptr<GlobalSourceState> sort_source;

	// Unsorted
	unique_ptr<ColumnDataCollection> columns;
	atomic<idx_t> get_columns;
};

HashedSortGroup::HashedSortGroup(ClientContext &client, optional_ptr<Sort> sort, idx_t group_idx)
    : group_idx(group_idx), count(0), sort(sort), tasks_completed(0), get_columns(0) {
	if (sort) {
		sort_global = sort->GetGlobalSinkState(client);
	}
}

//===--------------------------------------------------------------------===//
// HashedSortGlobalSinkState
//===--------------------------------------------------------------------===//
// Formerly PartitionGlobalSinkState
class HashedSortGlobalSinkState : public GlobalSinkState {
public:
	using HashGroupPtr = unique_ptr<HashedSortGroup>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	using GroupingPartition = unique_ptr<RadixPartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	HashedSortGlobalSinkState(ClientContext &client, const HashedSort &hashed_sort);

	// OVER(PARTITION BY...) (hash grouping)
	unique_ptr<RadixPartitionedTupleData> CreatePartition(idx_t new_bits) const;
	void SyncPartitioning(const HashedSortGlobalSinkState &other);
	void UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &partition_append);
	void CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
	ProgressData GetSinkProgress(ClientContext &context, const ProgressData source_progress) const;

	//! System and query state
	const HashedSort &hashed_sort;
	BufferManager &buffer_manager;
	Allocator &allocator;
	mutable mutex lock;

	// OVER(PARTITION BY...) (hash grouping)
	GroupingPartition grouping_data;
	//! Payload plus hash column
	shared_ptr<TupleDataLayout> grouping_types_ptr;
	//! The number of radix bits if this partition is being synced with another
	idx_t fixed_bits;
	vector<column_t> scan_ids;

	// OVER(...) (sorting)
	vector<HashGroupPtr> hash_groups;

	// Threading
	idx_t max_bits;
	atomic<idx_t> count;

private:
	void Rehash(idx_t cardinality);
	void SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
};

HashedSortGlobalSinkState::HashedSortGlobalSinkState(ClientContext &client, const HashedSort &hashed_sort)
    : hashed_sort(hashed_sort), buffer_manager(BufferManager::GetBufferManager(client)),
      allocator(Allocator::Get(client)), fixed_bits(0), max_bits(1), count(0) {
	const auto memory_per_thread = PhysicalOperator::GetMaxThreadMemory(client);
	const auto thread_pages = PreviousPowerOfTwo(memory_per_thread / (4 * buffer_manager.GetBlockAllocSize()));
	while (max_bits < 8 && (thread_pages >> max_bits) > 1) {
		++max_bits;
	}

	grouping_types_ptr = make_shared_ptr<TupleDataLayout>();
	auto &partitions = hashed_sort.partitions;
	auto &orders = hashed_sort.orders;
	auto &payload_types = hashed_sort.payload_types;
	if (!orders.empty()) {
		if (partitions.empty()) {
			//	Sort early into a dedicated hash group if we only sort.
			grouping_types_ptr->Initialize(payload_types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			auto new_group = make_uniq<HashedSortGroup>(hashed_sort.client, *hashed_sort.sort, idx_t(0));
			hash_groups.emplace_back(std::move(new_group));
		} else {
			auto types = payload_types;
			types.push_back(LogicalType::HASH);
			grouping_types_ptr->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
			Rehash(hashed_sort.estimated_cardinality);
			for (column_t i = 0; i < payload_types.size(); ++i) {
				scan_ids.emplace_back(i);
			}
		}
	}
}

unique_ptr<RadixPartitionedTupleData> HashedSortGlobalSinkState::CreatePartition(idx_t new_bits) const {
	auto &payload_types = hashed_sort.payload_types;
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
	local_partition->Repartition(hashed_sort.client, *new_partition);

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

void HashedSortGlobalSinkState::SyncPartitioning(const HashedSortGlobalSinkState &other) {
	fixed_bits = other.grouping_data ? other.grouping_data->GetRadixBits() : 0;

	const auto old_bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	if (fixed_bits != old_bits) {
		grouping_data = CreatePartition(fixed_bits);
	}
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
			hash_group = make_uniq<HashedSortGroup>(hashed_sort.client, *hashed_sort.sort, group_idx);
		}
	}

	//	Combine the thread data into the global data
	grouping_data->Combine(*local_partition);
}

ProgressData HashedSortGlobalSinkState::GetSinkProgress(ClientContext &client, const ProgressData source) const {
	ProgressData result;
	result.done = source.done / 2;
	result.total = source.total;
	result.invalid = source.invalid;

	// Sort::GetSinkProgress assumes that there is only 1 sort.
	// So we just use it to figure out how many rows have been sorted.
	const ProgressData zero_progress;
	lock_guard<mutex> guard(lock);
	const auto &sort = hashed_sort.sort;
	for (auto &hash_group : hash_groups) {
		if (!hash_group || !hash_group->sort_global) {
			continue;
		}

		const auto group_progress = sort->GetSinkProgress(client, *hash_group->sort_global, zero_progress);
		result.done += group_progress.done;
		result.invalid = result.invalid || group_progress.invalid;
	}

	return result;
}

SinkFinalizeType HashedSort::Finalize(ClientContext &client, OperatorSinkFinalizeInput &finalize) const {
	auto &gsink = finalize.global_state.Cast<HashedSortGlobalSinkState>();

	//	Did we get any data?
	if (!gsink.count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// OVER()
	if (!sort) {
		return SinkFinalizeType::READY;
	}

	//	OVER(ORDER BY...)
	if (partitions.empty()) {
		auto &hash_group = gsink.hash_groups[0];
		if (hash_group) {
			auto &global_sink = *hash_group->sort_global;
			OperatorSinkFinalizeInput hfinalize {global_sink, finalize.interrupt_state};
			sort->Finalize(client, hfinalize);
			hash_group->sort_source = sort->GetGlobalSourceState(client, global_sink);
			return SinkFinalizeType::READY;
		}
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// OVER(PARTITION BY...)
	auto &partitions = gsink.grouping_data->GetPartitions();
	D_ASSERT(!gsink.hash_groups.empty());
	for (hash_t hash_bin = 0; hash_bin < partitions.size(); ++hash_bin) {
		auto &partition = *partitions[hash_bin];
		if (!partition.Count()) {
			continue;
		}

		auto &hash_group = gsink.hash_groups[hash_bin];
		if (!hash_group) {
			continue;
		}

		// Prepare to scan into the sort
		auto &parallel_scan = hash_group->parallel_scan;
		partition.InitializeScan(parallel_scan, gsink.scan_ids);
	}

	return SinkFinalizeType::READY;
}

ProgressData HashedSort::GetSinkProgress(ClientContext &client, GlobalSinkState &gstate,
                                         const ProgressData source) const {
	auto &gsink = gstate.Cast<HashedSortGlobalSinkState>();
	return gsink.GetSinkProgress(client, source);
}

//===--------------------------------------------------------------------===//
// HashedSortLocalSinkState
//===--------------------------------------------------------------------===//
// Formerly PartitionLocalSinkState
class HashedSortLocalSinkState : public LocalSinkState {
public:
	using LocalSortStatePtr = unique_ptr<LocalSinkState>;
	using GroupingPartition = unique_ptr<RadixPartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	HashedSortLocalSinkState(ExecutionContext &context, const HashedSort &hashed_sort);

	//! Global state
	const HashedSort &hashed_sort;
	Allocator &allocator;

	//! Shared expression evaluation
	ExpressionExecutor hash_exec;
	ExpressionExecutor sort_exec;
	DataChunk group_chunk;
	DataChunk sort_chunk;
	DataChunk payload_chunk;

	//! Compute the hash values
	void Hash(DataChunk &input_chunk, Vector &hash_vector);
	//! Merge the state into the global state.
	void Combine(ExecutionContext &context);

	// OVER(PARTITION BY...) (hash grouping)
	GroupingPartition local_grouping;
	GroupingAppend grouping_append;

	// OVER(ORDER BY...) (only sorting)
	LocalSortStatePtr sort_local;

	// OVER() (no sorting)
	unique_ptr<ColumnDataCollection> unsorted;
	ColumnDataAppendState unsorted_append;
};

HashedSortLocalSinkState::HashedSortLocalSinkState(ExecutionContext &context, const HashedSort &hashed_sort)
    : hashed_sort(hashed_sort), allocator(Allocator::Get(context.client)), hash_exec(context.client),
      sort_exec(context.client) {
	vector<LogicalType> group_types;
	for (idx_t prt_idx = 0; prt_idx < hashed_sort.partitions.size(); prt_idx++) {
		auto &pexpr = *hashed_sort.partitions[prt_idx].expression.get();
		group_types.push_back(pexpr.return_type);
		hash_exec.AddExpression(pexpr);
	}

	vector<LogicalType> sort_types;
	for (const auto &expr : hashed_sort.sort_exprs) {
		sort_types.emplace_back(expr->return_type);
		sort_exec.AddExpression(*expr);
	}
	sort_chunk.Initialize(context.client, sort_types);

	if (hashed_sort.sort_col_count) {
		auto payload_types = hashed_sort.payload_types;
		if (!group_types.empty()) {
			// OVER(PARTITION BY...)
			group_chunk.Initialize(allocator, group_types);
			payload_types.emplace_back(LogicalType::HASH);
		} else {
			// OVER(ORDER BY...)
			auto &sort = *hashed_sort.sort;
			sort_local = sort.GetLocalSinkState(context);
		}
		// OVER(...)
		payload_chunk.Initialize(allocator, payload_types);
	} else {
		unsorted = make_uniq<ColumnDataCollection>(context.client, hashed_sort.payload_types);
		unsorted->InitializeAppend(unsorted_append);
	}
}

void HashedSort::Synchronize(const GlobalSinkState &source, GlobalSinkState &target) const {
	auto &src = source.Cast<HashedSortGlobalSinkState>();
	auto &tgt = target.Cast<HashedSortGlobalSinkState>();
	tgt.SyncPartitioning(src);
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

SinkResultType HashedSort::Sink(ExecutionContext &context, DataChunk &input_chunk, OperatorSinkInput &sink) const {
	auto &gstate = sink.global_state.Cast<HashedSortGlobalSinkState>();
	auto &lstate = sink.local_state.Cast<HashedSortLocalSinkState>();
	gstate.count += input_chunk.size();

	// Window::Sink:
	// PartitionedTupleData::Append
	// Sort::Sink
	// ColumnDataCollection::Append

	// OVER()
	if (gstate.hashed_sort.sort_col_count == 0) {
		lstate.unsorted->Append(lstate.unsorted_append, input_chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	//	Payload prefix is the input data
	auto &payload_chunk = lstate.payload_chunk;
	payload_chunk.Reset();
	for (column_t i = 0; i < input_chunk.ColumnCount(); ++i) {
		payload_chunk.data[i].Reference(input_chunk.data[i]);
	}

	//	Compute any sort columns that are not references and append them to the end of the payload
	auto &sort_chunk = lstate.sort_chunk;
	auto &sort_exec = lstate.sort_exec;
	if (!sort_exprs.empty()) {
		sort_chunk.Reset();
		sort_exec.Execute(input_chunk, sort_chunk);
		for (column_t i = 0; i < sort_chunk.ColumnCount(); ++i) {
			payload_chunk.data[input_chunk.ColumnCount() + i].Reference(sort_chunk.data[i]);
		}
	}

	//	Append a forced payload column
	if (force_payload) {
		auto &vec = payload_chunk.data[input_chunk.ColumnCount() + sort_chunk.ColumnCount()];
		D_ASSERT(vec.GetType().id() == LogicalTypeId::BOOLEAN);
		vec.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vec, true);
	}

	payload_chunk.SetCardinality(input_chunk);

	//	OVER(ORDER BY...)
	auto &sort_local = lstate.sort_local;
	if (sort_local) {
		auto &hash_group = *gstate.hash_groups[0];
		OperatorSinkInput input {*hash_group.sort_global, *sort_local, sink.interrupt_state};
		sort->Sink(context, payload_chunk, input);
		hash_group.count += payload_chunk.size();
		return SinkResultType::NEED_MORE_INPUT;
	}

	// OVER(PARTITION BY...)
	auto &hash_vector = payload_chunk.data.back();
	lstate.Hash(input_chunk, hash_vector);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}

	auto &local_grouping = lstate.local_grouping;
	auto &grouping_append = lstate.grouping_append;
	gstate.UpdateLocalPartition(local_grouping, grouping_append);
	local_grouping->Append(*grouping_append, payload_chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType HashedSort::Combine(ExecutionContext &context, OperatorSinkCombineInput &combine) const {
	auto &gstate = combine.global_state.Cast<HashedSortGlobalSinkState>();
	auto &lstate = combine.local_state.Cast<HashedSortLocalSinkState>();

	// Window::Combine:
	// Sort::Sink then Sort::Combine (per hash partition)
	// Sort::Combine
	// ColumnDataCollection::Combine

	// OVER()
	if (gstate.hashed_sort.sort_col_count == 0) {
		// Only one partition again, so need a global lock.
		lock_guard<mutex> glock(gstate.lock);
		auto &hash_groups = gstate.hash_groups;
		if (!hash_groups.empty()) {
			D_ASSERT(hash_groups.size() == 1);
			auto &hash_group = *hash_groups[0];
			auto &unsorted = *hash_group.columns;
			if (lstate.unsorted) {
				hash_group.count += lstate.unsorted->Count();
				unsorted.Combine(*lstate.unsorted);
				lstate.unsorted.reset();
			}
		} else {
			auto new_group = make_uniq<HashedSortGroup>(context.client, sort, idx_t(0));
			new_group->columns = std::move(lstate.unsorted);
			new_group->count += new_group->columns->Count();
			hash_groups.emplace_back(std::move(new_group));
		}
		return SinkCombineResultType::FINISHED;
	}

	//	OVER(ORDER BY...)
	if (lstate.sort_local) {
		auto &hash_group = *gstate.hash_groups[0];
		OperatorSinkCombineInput input {*hash_group.sort_global, *lstate.sort_local, combine.interrupt_state};
		sort->Combine(context, input);
		lstate.sort_local.reset();
		return SinkCombineResultType::FINISHED;
	}

	// OVER(PARTITION BY...)
	auto &local_grouping = lstate.local_grouping;
	if (!local_grouping) {
		return SinkCombineResultType::FINISHED;
	}

	// Flush our data and lock the bit count
	auto &grouping_append = lstate.grouping_append;
	gstate.CombineLocalPartition(local_grouping, grouping_append);

	return SinkCombineResultType::FINISHED;
}

void HashedSort::SortColumnData(ExecutionContext &context, hash_t hash_bin, OperatorSinkFinalizeInput &finalize) {
	auto &gstate = finalize.global_state.Cast<HashedSortGlobalSinkState>();

	// OVER()
	if (sort_col_count == 0) {
		//	Nothing to sort
		return;
	}

	//	OVER(ORDER BY...)
	if (partitions.empty()) {
		//	Already sorted in Combine
		return;
	}

	//	Loop over the partitions and add them to each hash group's global sort state
	auto &partitions = gstate.grouping_data->GetPartitions();
	if (hash_bin < partitions.size()) {
		auto &partition = *partitions[hash_bin];
		if (!partition.Count()) {
			return;
		}

		auto &hash_group = *gstate.hash_groups[hash_bin];
		auto &parallel_scan = hash_group.parallel_scan;

		DataChunk chunk;
		partition.InitializeScanChunk(parallel_scan.scan_state, chunk);
		TupleDataLocalScanState local_scan;
		partition.InitializeScan(local_scan);

		auto sort_local = sort->GetLocalSinkState(context);
		OperatorSinkInput sink {*hash_group.sort_global, *sort_local, finalize.interrupt_state};
		idx_t combined = 0;
		while (partition.Scan(hash_group.parallel_scan, local_scan, chunk)) {
			sort->Sink(context, chunk, sink);
			combined += chunk.size();
		}

		OperatorSinkCombineInput combine {*hash_group.sort_global, *sort_local, finalize.interrupt_state};
		sort->Combine(context, combine);
		hash_group.count += combined;

		//	Whoever finishes last can Finalize
		lock_guard<mutex> finalize_guard(hash_group.scan_lock);
		if (hash_group.count == partition.Count() && !hash_group.sort_source) {
			OperatorSinkFinalizeInput lfinalize {*hash_group.sort_global, finalize.interrupt_state};
			sort->Finalize(context.client, lfinalize);
			hash_group.sort_source = sort->GetGlobalSourceState(client, *hash_group.sort_global);
		}
	}
}

//===--------------------------------------------------------------------===//
// HashedSortGlobalSourceState
//===--------------------------------------------------------------------===//
class HashedSortGlobalSourceState : public GlobalSourceState {
public:
	using HashGroupPtr = unique_ptr<ColumnDataCollection>;
	using SortedRunPtr = unique_ptr<SortedRun>;
	using ChunkRow = HashedSort::ChunkRow;
	using ChunkRows = HashedSort::ChunkRows;

	HashedSortGlobalSourceState(ClientContext &client, HashedSortGlobalSinkState &gsink);

	HashedSortGlobalSinkState &gsink;
	ChunkRows chunk_rows;
};

HashedSortGlobalSourceState::HashedSortGlobalSourceState(ClientContext &client, HashedSortGlobalSinkState &gsink)
    : gsink(gsink) {
	if (!gsink.count) {
		return;
	}

	auto &hashed_sort = gsink.hashed_sort;

	// OVER()
	if (hashed_sort.sort_col_count == 0) {
		//	One unsorted group. We have the count and chunks.
		ChunkRow chunk_row;

		auto &hash_group = gsink.hash_groups[0];
		if (hash_group) {
			chunk_row.count = hash_group->count;
			chunk_row.chunks = hash_group->columns->ChunkCount();
		}

		chunk_rows.emplace_back(chunk_row);
		return;
	}

	//	OVER(ORDER BY...)
	if (hashed_sort.partitions.empty()) {
		//	One sorted group
		ChunkRow chunk_row;

		auto &hash_group = gsink.hash_groups[0];
		if (hash_group) {
			chunk_row.count = hash_group->count;
			chunk_row.chunks = (chunk_row.count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
		}

		chunk_rows.emplace_back(chunk_row);
		return;
	}

	//	OVER(PARTITION BY...)
	auto &partitions = gsink.grouping_data->GetPartitions();
	for (hash_t hash_bin = 0; hash_bin < partitions.size(); ++hash_bin) {
		ChunkRow chunk_row;

		auto &hash_group = gsink.hash_groups[hash_bin];
		if (hash_group) {
			chunk_row.count = partitions[hash_bin]->Count();
			chunk_row.chunks = (chunk_row.count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
		}

		chunk_rows.emplace_back(chunk_row);
	}
}

//===--------------------------------------------------------------------===//
// HashedSort
//===--------------------------------------------------------------------===//
void HashedSort::GenerateOrderings(Orders &partitions, Orders &orders,
                                   const vector<unique_ptr<Expression>> &partition_bys, const Orders &order_bys,
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

HashedSort::HashedSort(ClientContext &client, const vector<unique_ptr<Expression>> &partition_bys,
                       const vector<BoundOrderByNode> &order_bys, const Types &input_types,
                       const vector<unique_ptr<BaseStatistics>> &partition_stats, idx_t estimated_cardinality,
                       bool require_payload)
    : client(client), estimated_cardinality(estimated_cardinality), payload_types(input_types) {
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

	sort_col_count = orders.size() + partitions.size();
	if (sort_col_count) {
		// If a payload column is required, check whether there is one already
		if (require_payload) {
			//	Watch out for duplicate sort keys!
			unordered_set<column_t> sort_set(sort_ids.begin(), sort_ids.end());
			force_payload = (sort_set.size() >= payload_types.size());
			if (force_payload) {
				payload_types.emplace_back(LogicalType::BOOLEAN);
			}
		}
		vector<idx_t> projection_map;
		sort = make_uniq<Sort>(client, orders, payload_types, projection_map);
	}
}

unique_ptr<GlobalSinkState> HashedSort::GetGlobalSinkState(ClientContext &client) const {
	return make_uniq<HashedSortGlobalSinkState>(client, *this);
}

unique_ptr<LocalSinkState> HashedSort::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<HashedSortLocalSinkState>(context, *this);
}

unique_ptr<GlobalSourceState> HashedSort::GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const {
	return make_uniq<HashedSortGlobalSourceState>(client, sink.Cast<HashedSortGlobalSinkState>());
}

unique_ptr<LocalSourceState> HashedSort::GetLocalSourceState(ExecutionContext &context,
                                                             GlobalSourceState &gstate) const {
	return make_uniq<LocalSourceState>();
}

const HashedSort::ChunkRows &HashedSort::GetHashGroups(GlobalSourceState &gstate) const {
	auto &gsource = gstate.Cast<HashedSortGlobalSourceState>();
	return gsource.chunk_rows;
}

static SourceResultType MaterializeHashGroupData(ExecutionContext &context, idx_t hash_bin, bool build_runs,
                                                 OperatorSourceInput &source) {
	auto &gsource = source.global_state.Cast<HashedSortGlobalSourceState>();
	auto &gsink = gsource.gsink;
	auto &hash_group = *gsink.hash_groups[hash_bin];

	// OVER()
	if (gsink.hashed_sort.sort_col_count == 0) {
		D_ASSERT(hash_bin == 0);
		// Hack: Only report finished for the first call
		return hash_group.get_columns++ ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
	}

	//	OVER(PARTITION BY...)
	if (gsink.grouping_data) {
		lock_guard<mutex> reset_guard(hash_group.scan_lock);
		auto &partitions = gsink.grouping_data->GetPartitions();
		if (hash_bin < partitions.size()) {
			//	Release the memory now that we have finished scanning it.
			partitions[hash_bin].reset();
		}
	}

	auto &sort = *hash_group.sort;
	auto &sort_global = *hash_group.sort_source;
	auto sort_local = sort.GetLocalSourceState(context, sort_global);

	OperatorSourceInput input {sort_global, *sort_local, source.interrupt_state};
	if (build_runs) {
		return sort.MaterializeSortedRun(context, input);
	} else {
		return sort.MaterializeColumnData(context, input);
	}
}

SourceResultType HashedSort::MaterializeColumnData(ExecutionContext &execution, idx_t hash_bin,
                                                   OperatorSourceInput &source) const {
	return MaterializeHashGroupData(execution, hash_bin, false, source);
}

HashedSort::HashGroupPtr HashedSort::GetColumnData(idx_t hash_bin, OperatorSourceInput &source) const {
	auto &gsource = source.global_state.Cast<HashedSortGlobalSourceState>();
	auto &gsink = gsource.gsink;
	auto &hash_group = *gsink.hash_groups[hash_bin];

	// OVER()
	if (sort_col_count == 0) {
		D_ASSERT(hash_bin == 0);
		return std::move(hash_group.columns);
	}

	auto &sort = *hash_group.sort;
	auto &sort_global = *hash_group.sort_source;

	OperatorSourceInput input {sort_global, source.local_state, source.interrupt_state};
	auto result = sort.GetColumnData(input);
	hash_group.sort_source.reset();

	//	Just because MaterializeColumnData returned FINISHED doesn't mean that the same thread will
	//	get the result...
	if (result && result->Count() == hash_group.count) {
		return result;
	}

	return nullptr;
}

SourceResultType HashedSort::MaterializeSortedRun(ExecutionContext &context, idx_t hash_bin,
                                                  OperatorSourceInput &source) const {
	return MaterializeHashGroupData(context, hash_bin, true, source);
}

HashedSort::SortedRunPtr HashedSort::GetSortedRun(ClientContext &client, idx_t hash_bin,
                                                  OperatorSourceInput &source) const {
	auto &gsource = source.global_state.Cast<HashedSortGlobalSourceState>();
	auto &gsink = gsource.gsink;
	auto &hash_group = *gsink.hash_groups[hash_bin];

	D_ASSERT(gsink.hashed_sort.sort_col_count);

	auto &sort = *hash_group.sort;
	auto &sort_global = *hash_group.sort_source;

	auto result = sort.GetSortedRun(sort_global);
	if (!result) {
		D_ASSERT(hash_group.count == 0);
		result = make_uniq<SortedRun>(client, sort, false);
	}

	hash_group.sort_source.reset();

	return result;
}

} // namespace duckdb
