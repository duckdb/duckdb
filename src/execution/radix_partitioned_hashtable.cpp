#include "duckdb/execution/radix_partitioned_hashtable.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

RadixPartitionedHashTable::RadixPartitionedHashTable(GroupingSet &grouping_set_p, const GroupedAggregateData &op_p)
    : grouping_set(grouping_set_p), op(op_p) {
	auto groups_count = op.GroupCount();
	for (idx_t i = 0; i < groups_count; i++) {
		if (grouping_set.find(i) == grouping_set.end()) {
			null_groups.push_back(i);
		}
	}
	if (grouping_set.empty()) {
		// Fake a single group with a constant value for aggregation without groups
		group_types.emplace_back(LogicalType::TINYINT);
	}
	for (auto &entry : grouping_set) {
		D_ASSERT(entry < op.group_types.size());
		group_types.push_back(op.group_types[entry]);
	}
	SetGroupingValues();

	auto group_types_copy = group_types;
	group_types_copy.emplace_back(LogicalType::HASH);
	layout.Initialize(std::move(group_types_copy), AggregateObject::CreateAggregateObjects(op.bindings));
}

void RadixPartitionedHashTable::SetGroupingValues() {
	// Compute the GROUPING values:
	// For each parameter to the GROUPING clause, we check if the hash table groups on this particular group
	// If it does, we return 0, otherwise we return 1
	// We then use bitshifts to combine these values
	auto &grouping_functions = op.GetGroupingFunctions();
	for (auto &grouping : grouping_functions) {
		int64_t grouping_value = 0;
		D_ASSERT(grouping.size() < sizeof(int64_t) * 8);
		for (idx_t i = 0; i < grouping.size(); i++) {
			if (grouping_set.find(grouping[i]) == grouping_set.end()) {
				// We don't group on this value!
				grouping_value += (int64_t)1 << (grouping.size() - (i + 1));
			}
		}
		grouping_values.push_back(Value::BIGINT(grouping_value));
	}
}

const TupleDataLayout &RadixPartitionedHashTable::GetLayout() const {
	return layout;
}

unique_ptr<GroupedAggregateHashTable> RadixPartitionedHashTable::CreateHT(ClientContext &context, const idx_t capacity,
                                                                          const idx_t radix_bits) const {
	return make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), group_types, op.payload_types,
	                                            op.bindings, capacity, radix_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
//! Config for RadixPartitionedHashTable
struct RadixHTConfig {
public:
	RadixHTConfig(ClientContext &context, const idx_t row_width)
	    : sink_capacity(SinkCapacity(row_width)), sink_radix_bits(SinkRadixBits(context)),
	      repartition_radix_bits(RepartitionRadixBits(sink_radix_bits)) {
	}

private:
	static idx_t SinkCapacity(const idx_t row_width) {
		const auto size_per_row = sizeof(aggr_ht_entry_t) * GroupedAggregateHashTable::LOAD_FACTOR + row_width;
		const auto capacity = NextPowerOfTwo(CACHE_SIZE / size_per_row);
		return MaxValue<idx_t>(capacity, GroupedAggregateHashTable::InitialCapacity());
	}

	static idx_t SinkRadixBits(ClientContext &context) {
		const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		return RadixPartitioning::RadixBits(NextPowerOfTwo(n_threads));
	}

	static idx_t RepartitionRadixBits(idx_t sink_radix_bits_p) {
		return MinValue<idx_t>(sink_radix_bits_p + REPARTITION_RADIX_BITS, RadixPartitioning::MAX_RADIX_BITS);
	}

private:
	//! Assume (1 << 20) + (1 << 19) = 1.5MB cache per CPU
	static constexpr const idx_t CACHE_SIZE = 1572864;
	//! By how many bits to repartition if we go out-of-core
	static constexpr const idx_t REPARTITION_RADIX_BITS = 3;

public:
	//! Capacity of HTs during the Sink
	const idx_t sink_capacity;
	//! Radix bits used during the Sink
	const idx_t sink_radix_bits;
	//! Radix bits if we go out-of-core
	const idx_t repartition_radix_bits;
};

struct AggregatePartition {
	explicit AggregatePartition(unique_ptr<TupleDataCollection> data_p) : data(std::move(data_p)), finalized(false) {
	}
	unique_ptr<TupleDataCollection> data;
	atomic<bool> finalized;
};

class RadixHTGlobalSinkState : public GlobalSinkState {
public:
	RadixHTGlobalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Destroys aggregate states (if multi-scan)
	~RadixHTGlobalSinkState() override;
	void Destroy();

public:
	//! The radix HT
	const RadixPartitionedHashTable &radix_ht;
	//! Config for partitioning
	const RadixHTConfig config;

	//! Whether we've called Finalize
	bool finalized;
	//! Whether we are doing an external aggregation
	atomic<bool> external;
	//! Threads that have called Sink
	atomic<idx_t> active_threads;
	//! Threads that have called Combine
	atomic<idx_t> combined_threads;

	//! Lock for uncombined_data/stored_allocators
	mutex lock;
	//! Uncombined partitioned data that will be put into the AggregatePartitions
	unique_ptr<PartitionedTupleData> uncombined_data;
	//! Allocators used during the Sink/Finalize
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

	//! Partitions that are finalized during GetData
	vector<unique_ptr<AggregatePartition>> partitions;

	//! For synchronizing finalize tasks
	atomic<idx_t> finalize_idx;
	atomic<idx_t> finalize_done;

	//! Pin properties when scanning
	TupleDataPinProperties scan_pin_properties;
	//! Total count before combining
	idx_t count_before_combining;
};

RadixHTGlobalSinkState::RadixHTGlobalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht_p)
    : radix_ht(radix_ht_p), config(context, radix_ht.GetLayout().GetRowWidth()), finalized(false),
      external(context.config.force_external), active_threads(0), combined_threads(0), finalize_idx(0),
      finalize_done(0), scan_pin_properties(TupleDataPinProperties::DESTROY_AFTER_DONE), count_before_combining(0) {
}

RadixHTGlobalSinkState::~RadixHTGlobalSinkState() {
	Destroy();
}

void RadixHTGlobalSinkState::Destroy() {
	if (scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE || count_before_combining == 0 ||
	    partitions.empty()) {
		// Already destroyed / empty
		return;
	}

	TupleDataLayout layout = partitions[0]->data->GetLayout().Copy();
	if (!layout.HasDestructor()) {
		return; // No destructors, exit
	}

	// There are aggregates with destructors: Call the destructor for each of the aggregates
	RowOperationsState row_state(*stored_allocators.back());
	for (auto &partition : partitions) {
		auto &data_collection = *partition->data;
		if (data_collection.Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
		auto &row_locations = iterator.GetChunkState().row_locations;
		do {
			RowOperations::DestroyStates(row_state, layout, row_locations, iterator.GetCurrentChunkCount());
		} while (iterator.Next());
		data_collection.Reset();
	}
}

class RadixHTLocalSinkState : public LocalSinkState {
public:
	RadixHTLocalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	void Repartition(ClientContext &context, RadixHTGlobalSinkState &gstate);

public:
	//! Thread-local HT that is re-used after abandoning
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Chunk with group columns
	DataChunk group_chunk;

	//! Data that is abandoned ends up here first
	unique_ptr<PartitionedTupleData> repartitioned_data;
};

RadixHTLocalSinkState::RadixHTLocalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht) {
	// If there are no groups we create a fake group so everything has the same group
	group_chunk.InitializeEmpty(radix_ht.group_types);
	if (radix_ht.grouping_set.empty()) {
		group_chunk.data[0].Reference(Value::TINYINT(42));
	}
}

void RadixHTLocalSinkState::Repartition(ClientContext &context, RadixHTGlobalSinkState &gstate) {
	D_ASSERT(gstate.external);
	if (!repartitioned_data) {
		repartitioned_data = make_uniq<RadixPartitionedTupleData>(
		    BufferManager::GetBufferManager(context), gstate.radix_ht.GetLayout(), gstate.config.repartition_radix_bits,
		    gstate.radix_ht.GetLayout().ColumnCount() - 1);
	}
	ht->UnpinData();
	ht->GetPartitionedData()->Repartition(*repartitioned_data);
	ht->InitializePartitionedData();
}

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RadixHTGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> RadixPartitionedHashTable::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RadixHTLocalSinkState>(context.client, *this);
}

void RadixPartitionedHashTable::PopulateGroupChunk(DataChunk &group_chunk, DataChunk &input_chunk) const {
	idx_t chunk_index = 0;
	// Populate the group_chunk
	for (auto &group_idx : grouping_set) {
		// Retrieve the expression containing the index in the input chunk
		auto &group = op.groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
		group_chunk.data[chunk_index++].Reference(input_chunk.data[bound_ref_expr.index]);
	}
	group_chunk.SetCardinality(input_chunk.size());
	group_chunk.Verify();
}

bool OverMemoryLimit(ClientContext &context, const PartitionedTupleData &ht_data) {
	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	const idx_t limit = BufferManager::GetBufferManager(context).GetMaxMemory();
	const idx_t thread_limit = 0.6 * limit / n_threads;
	return ht_data.SizeInBytes() > thread_limit;
}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                     DataChunk &payload_input, const unsafe_vector<idx_t> &filter) const {
	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		gstate.active_threads++;
		lstate.ht = CreateHT(context.client, gstate.config.sink_capacity, gstate.config.sink_radix_bits);
	}

	auto &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);

	auto &ht = *lstate.ht;
	ht.AddChunk(group_chunk, payload_input, filter);

	if (ht.Count() + STANDARD_VECTOR_SIZE < ht.ResizeThreshold()) {
		return; // We can fit another chunk
	}

	// 'Reset' the HT without taking its data, we can just keep appending to the same collection
	// This only works because we never resize the HT
	ht.ClearPointerTable();
	ht.ResetCount();

	// If the size of the data owned by the HT becomes too big, we repartition and assume we need to go out-of-core
	if (OverMemoryLimit(context.client, *ht.GetPartitionedData())) {
		gstate.external = true;
		lstate.Repartition(context.client, gstate);
		return;
	}

	// TODO: combine early and often
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		return;
	}

	// Loop until all threads have called Combine, or until 'external' has been set
	gstate.combined_threads++;
	while (gstate.combined_threads < gstate.active_threads) {
		if (gstate.external) {
			break;
		}
	}

	auto &ht = *lstate.ht;
	ht.UnpinData();

	unique_ptr<PartitionedTupleData> local_data;
	if (gstate.external) {
		lstate.Repartition(context.client, gstate);
		local_data = std::move(lstate.repartitioned_data);
	} else {
		local_data = std::move(ht.GetPartitionedData());
	}

	lock_guard<mutex> guard(gstate.lock);
	if (gstate.uncombined_data) {
		gstate.uncombined_data->Combine(*local_data.get());
	} else {
		gstate.uncombined_data = std::move(local_data);
	}
	gstate.stored_allocators.emplace_back(ht.GetAggregateAllocator());
}

void RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	// TODO: detect single-threaded case and move data directly to scan data??

	if (gstate.uncombined_data) {
		auto &uncombined_data = *gstate.uncombined_data;
		gstate.count_before_combining = uncombined_data.Count();

		auto &uncombined_partition_data = uncombined_data.GetPartitions();
		gstate.partitions.reserve(uncombined_partition_data.size());
		for (idx_t i = 0; i < uncombined_partition_data.size(); i++) {
			gstate.partitions.emplace_back(make_uniq<AggregatePartition>(std::move(uncombined_partition_data[i])));
		}
	} else {
		gstate.count_before_combining = 0;
	}

	gstate.finalized = true;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
idx_t RadixPartitionedHashTable::Count(GlobalSinkState &sink_p) const {
	const auto count = CountInternal(sink_p);
	return count == 0 && grouping_set.empty() ? 1 : count;
}

idx_t RadixPartitionedHashTable::CountInternal(GlobalSinkState &sink_p) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	return sink.count_before_combining;
}

void RadixPartitionedHashTable::SetMultiScan(GlobalSinkState &sink_p) {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	sink.scan_pin_properties = TupleDataPinProperties::UNPIN_AFTER_DONE;
}

enum class RadixHTSourceTaskType : uint8_t { NO_TASK, FINALIZE, SCAN };

class RadixHTLocalSourceState;

class RadixHTGlobalSourceState : public GlobalSourceState {
public:
	RadixHTGlobalSourceState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Assigns a task to a local source state
	bool AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate);

public:
	//! The client context
	ClientContext &context;
	//! For synchronizing the source phase
	atomic<bool> finished;

	//! Column ids for scanning
	vector<column_t> column_ids;

	//! For synchronizing scan tasks
	atomic<idx_t> scan_idx;
	atomic<idx_t> scan_done;
};

enum class RadixHTScanStatus : uint8_t { INIT, IN_PROGRESS, DONE };

class RadixHTLocalSourceState : public LocalSourceState {
public:
	explicit RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht);

public:
	//! Do the work this thread has been assigned
	void ExecuteTask(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished();

private:
	//! Execute the finalize or scan task
	void Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate);
	void Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);

public:
	//! Assigned task and index
	RadixHTSourceTaskType task;
	idx_t task_idx;
	//! Thread-local HT that is re-used
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Current status of a scan
	RadixHTScanStatus scan_status;

private:
	//! Allocator and layout for finalizing state
	TupleDataLayout layout;
	ArenaAllocator aggregate_allocator;

	//! State and chunk for scanning
	TupleDataScanState scan_state;
	DataChunk scan_chunk;
};

unique_ptr<GlobalSourceState> RadixPartitionedHashTable::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RadixHTGlobalSourceState>(context, *this);
}

unique_ptr<LocalSourceState> RadixPartitionedHashTable::GetLocalSourceState(ExecutionContext &context) const {
	return make_uniq<RadixHTLocalSourceState>(context, *this);
}

RadixHTGlobalSourceState::RadixHTGlobalSourceState(ClientContext &context_p, const RadixPartitionedHashTable &radix_ht)
    : context(context_p), finished(false), scan_idx(0), scan_done(0) {
	for (column_t column_id = 0; column_id < radix_ht.group_types.size(); column_id++) {
		column_ids.push_back(column_id);
	}
}

bool RadixHTGlobalSourceState::AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate) {
	D_ASSERT(lstate.scan_status != RadixHTScanStatus::IN_PROGRESS);

	const auto n_partitions = sink.partitions.size();
	if (scan_done == n_partitions) {
		finished = true;
		return false;
	}
	// We first try to assign a Scan task, then a Finalize task if that didn't work, without using any locks

	// We need an atomic compare-and-swap to assign a Scan task, because we need to only increment
	// the 'scan_idx' atomic if that partition's 'finalize' is true, i.e., ready to be scanned
	bool scan_assigned = true;
	do {
		lstate.task_idx = scan_idx.load();
		if (lstate.task_idx >= n_partitions || !sink.partitions[lstate.task_idx]->finalized) {
			scan_assigned = false;
			break;
		}
	} while (!std::atomic_compare_exchange_weak(&scan_idx, &lstate.task_idx, lstate.task_idx + 1));

	if (scan_assigned) {
		// We successfully assigned a Scan task
		D_ASSERT(lstate.task_idx < n_partitions && sink.partitions[lstate.task_idx]->finalized);
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.scan_status = RadixHTScanStatus::INIT;
		return true;
	}

	// We can just increment the atomic here, much simpler than assigning the scan task
	lstate.task_idx = sink.finalize_idx++;
	if (lstate.task_idx < n_partitions) {
		// We successfully assigned a Finalize task
		lstate.task = RadixHTSourceTaskType::FINALIZE;
		return true;
	}

	// We didn't manage to assign a finalize task
	return false;
}

RadixHTLocalSourceState::RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht)
    : task(RadixHTSourceTaskType::NO_TASK), scan_status(RadixHTScanStatus::DONE), layout(radix_ht.GetLayout().Copy()),
      aggregate_allocator(BufferAllocator::Get(context.client)) {
	auto &allocator = BufferAllocator::Get(context.client);
	auto scan_chunk_types = radix_ht.group_types;
	for (auto &aggr_type : radix_ht.op.aggregate_return_types) {
		scan_chunk_types.push_back(aggr_type);
	}
	scan_chunk.Initialize(allocator, scan_chunk_types);
}

void RadixHTLocalSourceState::ExecuteTask(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate,
                                          DataChunk &chunk) {
	switch (task) {
	case RadixHTSourceTaskType::FINALIZE:
		Finalize(sink, gstate);
		break;
	case RadixHTSourceTaskType::SCAN:
		Scan(sink, gstate, chunk);
		break;
	default:
		throw InternalException("Unexpected RadixHTSourceTaskType in ExecuteTask!");
	}
}

void RadixHTLocalSourceState::Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate) {
	D_ASSERT(task == RadixHTSourceTaskType::FINALIZE);
	D_ASSERT(scan_status != RadixHTScanStatus::IN_PROGRESS);

	auto &partition = *sink.partitions[task_idx];
	if (partition.data->Count() == 0) {
		partition.finalized = true;
		sink.finalize_done++;
		return;
	}

	if (!ht) {
		// Create a HT with sufficient capacity
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(partition.data->Count());
		ht = sink.radix_ht.CreateHT(gstate.context, capacity, 0);
	} else {
		// We might want to resize here to this partition's size, but for now we just assume uniform partition sizes
		ht->InitializePartitionedData();
		ht->ClearPointerTable();
		ht->ResetCount();
	}

	// Now combine the uncombined data using this thread's HT
	ht->Combine(*partition.data);
	ht->UnpinData();

	// Move the combined data back to the partition
	partition.data =
	    make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(gstate.context), sink.radix_ht.GetLayout());
	partition.data->Combine(*ht->GetPartitionedData()->GetPartitions()[0]);

	// Mark task as done
	partition.finalized = true;
	sink.finalize_done++;

	// Make sure this thread's aggregate allocator does not get lost
	lock_guard<mutex> guard(sink.lock);
	sink.stored_allocators.emplace_back(ht->GetAggregateAllocator());
}

void RadixHTLocalSourceState::Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk) {
	D_ASSERT(task == RadixHTSourceTaskType::SCAN);
	D_ASSERT(scan_status != RadixHTScanStatus::DONE);

	auto &partition = *sink.partitions[task_idx];
	D_ASSERT(partition.finalized);
	auto &data_collection = *partition.data;

	if (data_collection.Count() == 0) {
		scan_status = RadixHTScanStatus::DONE;
		gstate.scan_done++;
		return;
	}

	if (scan_status == RadixHTScanStatus::INIT) {
		data_collection.InitializeScan(scan_state, gstate.column_ids, sink.scan_pin_properties);
		scan_status = RadixHTScanStatus::IN_PROGRESS;
	}

	if (!data_collection.Scan(scan_state, scan_chunk)) {
		scan_status = RadixHTScanStatus::DONE;
		gstate.scan_done++;
		if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE) {
			data_collection.Reset();
		}
		return;
	}

	RowOperationsState row_state(aggregate_allocator);
	const auto group_cols = layout.ColumnCount() - 1;
	RowOperations::FinalizeStates(row_state, layout, scan_state.chunk_state.row_locations, scan_chunk, group_cols);

	if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE && layout.HasDestructor()) {
		RowOperations::DestroyStates(row_state, layout, scan_state.chunk_state.row_locations, scan_chunk.size());
	}

	auto &radix_ht = sink.radix_ht;
	idx_t chunk_index = 0;
	for (auto &entry : radix_ht.grouping_set) {
		chunk.data[entry].Reference(scan_chunk.data[chunk_index++]);
	}
	for (auto null_group : radix_ht.null_groups) {
		chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(chunk.data[null_group], true);
	}
	D_ASSERT(radix_ht.grouping_set.size() + radix_ht.null_groups.size() == radix_ht.op.GroupCount());
	for (idx_t col_idx = 0; col_idx < radix_ht.op.aggregates.size(); col_idx++) {
		chunk.data[radix_ht.op.GroupCount() + col_idx].Reference(
		    scan_chunk.data[radix_ht.group_types.size() + col_idx]);
	}
	D_ASSERT(radix_ht.op.grouping_functions.size() == radix_ht.grouping_values.size());
	for (idx_t i = 0; i < radix_ht.op.grouping_functions.size(); i++) {
		chunk.data[radix_ht.op.GroupCount() + radix_ht.op.aggregates.size() + i].Reference(radix_ht.grouping_values[i]);
	}
	chunk.SetCardinality(scan_chunk);
	D_ASSERT(chunk.size() != 0);
}

bool RadixHTLocalSourceState::TaskFinished() {
	switch (task) {
	case RadixHTSourceTaskType::FINALIZE:
		return true;
	case RadixHTSourceTaskType::SCAN:
		return scan_status == RadixHTScanStatus::DONE;
	default:
		D_ASSERT(task == RadixHTSourceTaskType::NO_TASK);
		return true;
	}
}

SourceResultType RadixPartitionedHashTable::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    GlobalSinkState &sink_p, OperatorSourceInput &input) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	D_ASSERT(sink.finalized);

	auto &gstate = input.global_state.Cast<RadixHTGlobalSourceState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSourceState>();
	D_ASSERT(sink.scan_pin_properties == TupleDataPinProperties::UNPIN_AFTER_DONE ||
	         sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE);

	if (gstate.finished) {
		return SourceResultType::FINISHED;
	}

	// Special case hack to sort out aggregating from empty intermediates for aggregations without groups
	if (CountInternal(sink_p) == 0 && grouping_set.empty()) {
		D_ASSERT(chunk.ColumnCount() == null_groups.size() + op.aggregates.size() + op.grouping_functions.size());
		// For each column in the aggregates, set to initial state
		chunk.SetCardinality(1);
		for (auto null_group : null_groups) {
			chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(chunk.data[null_group], true);
		}
		ArenaAllocator allocator(BufferAllocator::Get(context.client));
		for (idx_t i = 0; i < op.aggregates.size(); i++) {
			D_ASSERT(op.aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = op.aggregates[i]->Cast<BoundAggregateExpression>();
			auto aggr_state = make_unsafe_uniq_array<data_t>(aggr.function.state_size());
			aggr.function.initialize(aggr_state.get());

			AggregateInputData aggr_input_data(aggr.bind_info.get(), allocator);
			Vector state_vector(Value::POINTER(CastPointerToValue(aggr_state.get())));
			aggr.function.finalize(state_vector, aggr_input_data, chunk.data[null_groups.size() + i], 1, 0);
			if (aggr.function.destructor) {
				aggr.function.destructor(state_vector, aggr_input_data, 1);
			}
		}
		// Place the grouping values (all the groups of the grouping_set condensed into a single value)
		// Behind the null groups + aggregates
		for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
			chunk.data[null_groups.size() + op.aggregates.size() + i].Reference(grouping_values[i]);
		}
		gstate.finished = true;
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	while (!gstate.finished && chunk.size() == 0) {
		if (!lstate.TaskFinished() || gstate.AssignTask(sink, lstate)) {
			lstate.ExecuteTask(sink, gstate, chunk);
		}
	}

	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
