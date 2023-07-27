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

TupleDataLayout RadixPartitionedHashTable::GetLayout() const {
	auto group_types_copy = group_types;
	group_types_copy.emplace_back(LogicalType::HASH);
	TupleDataLayout layout;
	layout.Initialize(std::move(group_types_copy), AggregateObject::CreateAggregateObjects(op.bindings));
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
	explicit RadixHTConfig(ClientContext &context)
	    : overflow_radix_bits(NextPowerOfTwo(1.5 * TaskScheduler::GetScheduler(context).NumberOfThreads())) {
	}

public:
	idx_t OverflowPartitionCount() const {
		return RadixPartitioning::NumberOfPartitions(overflow_radix_bits);
	}

public:
	//! Radix bits used during the Sink
	static constexpr const idx_t SINK_RADIX_BITS = 3;
	//! Abandon HT after crossing this threshold
	static constexpr const idx_t SINK_ABANDON_THRESHOLD = (idx_t(1) << 16) / GroupedAggregateHashTable::LOAD_FACTOR;

	//! Radix bits used during the finalize
	const idx_t overflow_radix_bits;
};

struct AggregatePartition {
	AggregatePartition(ClientContext &context, const RadixPartitionedHashTable &radix_ht) {
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(RadixHTConfig::SINK_ABANDON_THRESHOLD);
		ht = radix_ht.CreateHT(context, capacity, 0);
		uncombined_data = make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(context), ht->GetLayout());
	}

	mutex lock;
	unique_ptr<GroupedAggregateHashTable> ht;
	unique_ptr<TupleDataCollection> uncombined_data;
};

class RadixHTGlobalSinkState : public GlobalSinkState {
public:
	RadixHTGlobalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Destroys aggregate states (if multi-scan)
	~RadixHTGlobalSinkState() override;
	void Destroy();

public:
	//! TODO
	const RadixHTConfig config;

	mutex lock;

	//! Uncombined abandoned overflow data
	unique_ptr<PartitionedTupleData> overflow_data;
	//! Allocators used during the Sink/Finalize
	vector<shared_ptr<ArenaAllocator>> stored_allocators;

	//! Partitions that data will be combined into
	vector<unique_ptr<AggregatePartition>> overflow_partitions;

	//! Whether we've called Finalize
	bool finalized;

	//! For synchronizing finalize tasks
	idx_t finalize_idx;
	atomic<idx_t> finalize_done;

	//! Pin properties when scanning
	TupleDataPinProperties scan_pin_properties;
	//! The final data that has to be scanned
	vector<unique_ptr<TupleDataCollection>> scan_data;
	//! Total count before combining
	idx_t count_before_combining;
};

RadixHTGlobalSinkState::RadixHTGlobalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht)
    : config(context), finalized(false), finalize_idx(0), finalize_done(0),
      scan_pin_properties(TupleDataPinProperties::DESTROY_AFTER_DONE), count_before_combining(0) {

	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto layout = radix_ht.GetLayout();
	overflow_data = make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, idx_t(config.overflow_radix_bits),
	                                                     layout.ColumnCount() - 1);

	overflow_partitions.resize(config.OverflowPartitionCount());
}

RadixHTGlobalSinkState::~RadixHTGlobalSinkState() {
	Destroy();
}

void RadixHTGlobalSinkState::Destroy() {
	if (scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE || scan_data.empty()) {
		// Already destroyed / empty
		return;
	}

	auto layout = scan_data.back()->GetLayout().Copy();
	RowOperationsState row_state(*stored_allocators.back());
	for (auto &data_collection : scan_data) {
		// There are aggregates with destructors: Call the destructor for each of the aggregates

		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
		auto &row_locations = iterator.GetChunkState().row_locations;
		do {
			RowOperations::DestroyStates(row_state, layout, row_locations, iterator.GetCurrentChunkCount());
		} while (iterator.Next());
		data_collection->Reset();
	}
}

class RadixHTLocalSinkState : public LocalSinkState {
public:
	RadixHTLocalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

public:
	//! Thread-local HT that is re-used after abandoning
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Chunk with group columns
	DataChunk group_chunk;

	//! Data that is abandoned ends up here first
	unique_ptr<PartitionedTupleData> overflow_data;
};

RadixHTLocalSinkState::RadixHTLocalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht) {
	// Init thread-local HT
	const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(RadixHTConfig::SINK_ABANDON_THRESHOLD);
	ht = radix_ht.CreateHT(context, capacity, RadixHTConfig::SINK_RADIX_BITS);

	// If there are no groups we create a fake group so everything has the same group
	group_chunk.InitializeEmpty(radix_ht.group_types);
	if (radix_ht.grouping_set.empty()) {
		group_chunk.data[0].Reference(Value::TINYINT(42));
	}
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

void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                     DataChunk &payload_input, const unsafe_vector<idx_t> &filter) const {
	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	if (!lstate.overflow_data) {
		// Initialize the overflow data
		const auto layout = GetLayout();
		lstate.overflow_data =
		    make_uniq<RadixPartitionedTupleData>(BufferManager::GetBufferManager(context.client), layout,
		                                         gstate.config.overflow_radix_bits, layout.ColumnCount() - 1);
	}

	auto &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);

	auto &ht = *lstate.ht;
	ht.AddChunk(group_chunk, payload_input, filter);

	if (ht.Count() + STANDARD_VECTOR_SIZE < RadixHTConfig::SINK_ABANDON_THRESHOLD) {
		return; // We can fit another chunk
	}

	// Abandon and repartition the data of the HT
	ht.UnpinData();
	ht.GetPartitionedData().Repartition(*lstate.overflow_data);
	ht.InitializePartitionedData();
	ht.ClearPointerTable();

	// TODO: combine early and often
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	auto &ht = *lstate.ht;

	// Abandon and repartition the data of the HT, then add to global state
	ht.UnpinData();
	ht.GetPartitionedData().Repartition(*lstate.overflow_data);
	gstate.overflow_data->Combine(*lstate.overflow_data);

	lock_guard<mutex> guard(gstate.lock);
	gstate.stored_allocators.emplace_back(ht.GetAggregateAllocator());
}

void RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	lock_guard<mutex> guard(gstate.lock);

	// TODO: detect single-threaded case and move data directly to scan data??

	auto &overflow_partitions = gstate.overflow_partitions;
	auto &data_partitions = gstate.overflow_data->GetPartitions();
	D_ASSERT(overflow_partitions.size() == data_partitions.size());

	auto &count_before_combining = gstate.count_before_combining;
	for (idx_t partition_idx = 0; partition_idx < gstate.config.OverflowPartitionCount(); partition_idx++) {
		auto &aggregate_partition = overflow_partitions[partition_idx];
		if (!aggregate_partition) {
			aggregate_partition = make_uniq<AggregatePartition>(context, *this);
		}
		aggregate_partition->uncombined_data->Combine(*data_partitions[partition_idx]);

		count_before_combining += aggregate_partition->ht->Count();
		count_before_combining += aggregate_partition->uncombined_data->Count();
	}
	gstate.overflow_data.reset();
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
	ClientContext &context;

	//! Column ids for scanning
	vector<column_t> column_ids;

	//! For synchronizing the source phase
	atomic<bool> finished;

	//! For synchronizing scan tasks
	idx_t scan_idx;
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
	//! Execute the finalize, or scan task
	void Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate);
	void Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);

public:
	//! Assigned task and index
	RadixHTSourceTaskType task;
	optional_idx task_idx;

	//! Thread-local HT that is re-used
	unique_ptr<GroupedAggregateHashTable> ht;

	//! Current data collection being scanned
	optional_ptr<TupleDataCollection> scan_collection;
	//! Current status of the scan
	RadixHTScanStatus scan_status;

private:
	const RadixPartitionedHashTable &radix_ht;

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

	lock_guard<mutex> guard(sink.lock);
	if (scan_done == sink.scan_data.size() && sink.finalize_done == RadixHTConfig::FinalizePartitionCount()) {
		finished = true;
	}
	if (finished) {
		return false;
	}

	// Try to assign a scan task first
	if (scan_idx < sink.scan_data.size()) {
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.task_idx = scan_idx++;
		lstate.scan_collection = sink.scan_data[lstate.task_idx.GetIndex()].get();
		lstate.scan_status = RadixHTScanStatus::INIT;
		return true;
	}

	// Try to assign a finalize task next
	if (sink.finalize_idx < RadixHTConfig::FinalizePartitionCount()) {
		lstate.task = RadixHTSourceTaskType::FINALIZE;
		lstate.task_idx = sink.finalize_idx++;
		return true;
	}

	return false;
}

RadixHTLocalSourceState::RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht_p)
    : task(RadixHTSourceTaskType::NO_TASK), scan_status(RadixHTScanStatus::DONE), radix_ht(radix_ht_p),
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

	auto &partition = *sink.partitions[task_idx.GetIndex()];
	auto &uncombined_data = *partition.uncombined_data;

	const auto count = partition.ht ? partition.ht->Count() + uncombined_data.Count() : uncombined_data.Count();
	if (count == 0) {
		sink.finalize_done++;
		return;
	}

	if (partition.ht) {
		ht = std::move(partition.ht);
		// Resize existing HT to sufficient capacity
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(count);
		if (ht->Capacity() < capacity) {
			ht->Resize(capacity);
		}
	} else if (!ht) {
		// Create a HT with sufficient capacity
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(partition.uncombined_data->Count());
		ht = radix_ht.CreateHT(gstate.context, capacity, 0);
	} else {
		D_ASSERT(ht->Count() == 0);
		// TODO maybe resize here??
		ht->ClearPointerTable();
	}

	// Now combine the data
	ht->Combine(uncombined_data);

	// Add to global state
	D_ASSERT(ht->GetPartitionedData().GetPartitions().size() == 1);
	D_ASSERT(ht->GetPartitionedData().GetPartitions()[0]->Count() != 0);
	lock_guard<mutex> guard(sink.lock);
	sink.scan_data.emplace_back(std::move(ht->GetPartitionedData().GetPartitions()[0]));
	sink.stored_allocators.emplace_back(ht->GetAggregateAllocator());

	// Mark task as done
	sink.finalize_done++;
}

void RadixHTLocalSourceState::Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk) {
	D_ASSERT(task == RadixHTSourceTaskType::SCAN);

	D_ASSERT(scan_collection->Count() != 0);
	if (scan_status == RadixHTScanStatus::INIT) {
		scan_collection->InitializeScan(scan_state, gstate.column_ids, sink.scan_pin_properties);
		scan_status = RadixHTScanStatus::IN_PROGRESS;
	}

	if (!scan_collection->Scan(scan_state, scan_chunk)) {
		scan_status = RadixHTScanStatus::DONE;
		if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE) {
			lock_guard<mutex> guard(sink.lock);
			sink.scan_data[task_idx.GetIndex()].reset();
		}
		gstate.scan_done++;
		return;
	}

	if (layout.GetTypes().empty()) {
		layout = scan_collection->GetLayout().Copy();
	}

	RowOperationsState row_state(aggregate_allocator);
	const auto group_cols = layout.ColumnCount() - 1;
	RowOperations::FinalizeStates(row_state, layout, scan_state.chunk_state.row_locations, scan_chunk, group_cols);

	if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE && layout.HasDestructor()) {
		RowOperations::DestroyStates(row_state, layout, scan_state.chunk_state.row_locations, scan_chunk.size());
	}

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
