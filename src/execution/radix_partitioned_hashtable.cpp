#include "duckdb/execution/radix_partitioned_hashtable.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <cmath>

namespace duckdb {

//! Config for RadixPartitionedHashTable
struct RadixHTConfig {
	//! Radix bits used during the Sink
	static constexpr const idx_t SINK_RADIX_BITS = 4;
	//! Check whether to abandon HT after crossing this threshold
	static constexpr const idx_t SINK_ABANDON_THRESHOLD = 500000;
	//! If we cross SINK_ABANDON_THRESHOLD, we decide whether to continue with the current HT or abandon it.
	//! Abandoning is better if the input has virtually no duplicates.
	//! Continuing is better if there are a significant amount of duplicates.
	//! If 524288 tuples went into our current HT, and there are 510583 uniques, do we abandon?
	//! Seems like we should, but if our input is random uniform, we are exactly on track to see 10.000.000 groups.
	//! If our input size is 100.000.000, then we see each tuple 10 times, and abandoning is actually a bad choice.
	//! All of this is to defend against our greatest enemy, the random uniform distribution with repetition.
	//! We keep track of the size of the HT, and the number of tuples that went into it.
	//! If we are on track to see 25x our current unique count, we can safely abandon the HTs early!
	static constexpr const idx_t SINK_EXPECTED_GROUP_COUNT_FACTOR = 25;

	//! The maximum number of groups per finalize task. We repartition if there could be more
	static constexpr const idx_t FINALIZE_MAX_GROUP_COUNT = 500000;
	//! For in-memory finalizes, we repartition using up to 8 radix bits
	static constexpr const idx_t FINALIZE_MAX_RADIX_BITS = 8;
};

// compute the GROUPING values
// for each parameter to the GROUPING clause, we check if the hash table groups on this particular group
// if it does, we return 0, otherwise we return 1
// we then use bitshifts to combine these values
void RadixPartitionedHashTable::SetGroupingValues() {
	auto &grouping_functions = op.GetGroupingFunctions();
	for (auto &grouping : grouping_functions) {
		int64_t grouping_value = 0;
		D_ASSERT(grouping.size() < sizeof(int64_t) * 8);
		for (idx_t i = 0; i < grouping.size(); i++) {
			if (grouping_set.find(grouping[i]) == grouping_set.end()) {
				// we don't group on this value!
				grouping_value += (int64_t)1 << (grouping.size() - (i + 1));
			}
		}
		grouping_values.push_back(Value::BIGINT(grouping_value));
	}
}

RadixPartitionedHashTable::RadixPartitionedHashTable(GroupingSet &grouping_set_p, const GroupedAggregateData &op_p)
    : grouping_set(grouping_set_p), op(op_p) {

	auto groups_count = op.GroupCount();
	for (idx_t i = 0; i < groups_count; i++) {
		if (grouping_set.find(i) == grouping_set.end()) {
			null_groups.push_back(i);
		}
	}

	if (grouping_set.empty()) {
		// fake a single group with a constant value for aggregation without groups
		group_types.emplace_back(LogicalType::TINYINT);
	}
	for (auto &entry : grouping_set) {
		D_ASSERT(entry < op.group_types.size());
		group_types.push_back(op.group_types[entry]);
	}
	SetGroupingValues();
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct MaterializedAggregateData {
	explicit MaterializedAggregateData(unique_ptr<TupleDataCollection> data_collection_p)
	    : data_collection(std::move(data_collection_p)) {
		D_ASSERT(data_collection);
	}
	unique_ptr<TupleDataCollection> data_collection;
	vector<shared_ptr<ArenaAllocator>> allocators;
};

struct AggregatePartition {
	AggregatePartition() : repartition_tasks_done(0), finalize_available(false) {
	}

	mutex lock;

	optional_idx count;
	vector<MaterializedAggregateData> uncombined_data;

	//! For synchronizing repartitioning
	optional_idx data_per_repartition_task;
	atomic<idx_t> repartition_tasks_done;

	//! For synchronizing finalizing
	atomic<bool> finalize_available;
};

class RadixHTGlobalSinkState : public GlobalSinkState {
public:
	explicit RadixHTGlobalSinkState(const RadixPartitionedHashTable &radix_ht_p)
	    : radix_ht(radix_ht_p), scan_pin_properties(TupleDataPinProperties::DESTROY_AFTER_DONE) {
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(RadixHTConfig::SINK_RADIX_BITS);
		sink_partitions.resize(num_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			sink_partitions[partition_idx] = make_uniq<AggregatePartition>();
		}
	}

	idx_t AddToFinal(GroupedAggregateHashTable &intermediate_ht, vector<MaterializedAggregateData> &uncombined_data) {
		unique_ptr<PartitionedTupleData> partitioned_data;
		shared_ptr<ArenaAllocator> aggregate_allocator;
		intermediate_ht.GetDataOwnership(partitioned_data, aggregate_allocator);
		D_ASSERT(partitioned_data->GetPartitions().size() == 1);

		auto &data_collection = partitioned_data->GetPartitions()[0];
		D_ASSERT(data_collection->Count() != 0);

		lock_guard<mutex> guard(lock);
		final_data.emplace_back(std::move(data_collection));
		final_data.back().allocators.emplace_back(aggregate_allocator);
		for (auto &ucb : uncombined_data) {
			D_ASSERT(!ucb.allocators.empty());
			for (auto &allocator : ucb.allocators) {
				final_data.back().allocators.emplace_back(allocator);
			}
		}
		return final_data.size() - 1;
	}

	void Destroy() {
		if (scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE) {
			return;
		}

		for (auto &data : final_data) {
			// There are aggregates with destructors: Call the destructor for each of the aggregates
			RowOperationsState row_state(*data.allocators.back());
			auto layout = data.data_collection->GetLayout().Copy();
			TupleDataChunkIterator iterator(*data.data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
			auto &row_locations = iterator.GetChunkState().row_locations;
			do {
				RowOperations::DestroyStates(row_state, layout, row_locations, iterator.GetCurrentChunkCount());
			} while (iterator.Next());
			data.data_collection->Reset();
		}
	}

	~RadixHTGlobalSinkState() {
		Destroy();
	}

	//! The HT object
	const RadixPartitionedHashTable &radix_ht;

	//! The radix partitions during the sink
	vector<unique_ptr<AggregatePartition>> sink_partitions;

	//! Radix bits used during the sink
	optional_idx finalize_radix_bits;
	//! Number of tasks per partition when repartitioning
	optional_idx repartition_tasks_per_partition;
	//! The radix partitions during the finalize
	vector<unique_ptr<AggregatePartition>> finalize_partitions;

	//! Lock for final stuff
	mutex lock;
	//! Pin properties when scanning
	TupleDataPinProperties scan_pin_properties;
	//! The final data that has to be scanned
	vector<MaterializedAggregateData> final_data;
	//! Total count of final_data
	optional_idx final_count;
};

class RadixHTLocalSinkState : public LocalSinkState {
public:
	explicit RadixHTLocalSinkState(const RadixPartitionedHashTable &ht) {
		// if there are no groups we create a fake group so everything has the same group
		group_chunk.InitializeEmpty(ht.group_types);
		if (ht.grouping_set.empty()) {
			group_chunk.data[0].Reference(Value::TINYINT(42));
		}
	}

	//! Thread-local HT that is re-used
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Chunk with group columns
	DataChunk group_chunk;
};

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &) const {
	return make_uniq<RadixHTGlobalSinkState>(*this);
}

unique_ptr<LocalSinkState> RadixPartitionedHashTable::GetLocalSinkState(ExecutionContext &) const {
	return make_uniq<RadixHTLocalSinkState>(*this);
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

static inline bool AbandonHT(ClientContext &context, const GroupedAggregateHashTable &ht) {
	if (ht.TotalSize() > double(0.6) * BufferManager::GetBufferManager(context).GetMaxMemory() /
	                         TaskScheduler::GetScheduler(context).NumberOfThreads()) {
		// Abandon to stay under memory limit
		return true;
	} else if (ht.Count() > RadixHTConfig::SINK_ABANDON_THRESHOLD) {
		// Math taken from https://math.stackexchange.com/a/1088094
		const double k = ht.Count() * RadixHTConfig::SINK_EXPECTED_GROUP_COUNT_FACTOR;
		const double n = ht.SinkCount();

		// Compute the expected number of groups after seeing 'n' tuples,
		// if the group count in the input would be equal to 'k'
		const auto ev = k * (1 - std::pow(1 - 1 / k, n));

		// Compute the variance of the expected number of groups
		const auto a = k * (k - 1) * std::pow(1 - 2 / k, n);
		const auto b = k * std::pow(1 - 1 / k, n);
		const auto c = std::pow(k, 2) * std::pow(1 - 1 / k, 2 * n);
		const auto var = a + b - c;

		// Compute the standard deviation
		const auto stdev = std::pow(AbsValue(var), 0.5);

		// With 3 standard deviations we're 99.9% sure we're headed not towards 'k' or more groups
		const auto threshold = ev - 3 * stdev;

		// Abandon because too many uniques
		return ht.Count() > threshold;
	}
	// Don't abandon
	return false;
}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                     DataChunk &payload_input, const unsafe_vector<idx_t> &filter) const {
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	auto &ht = lstate.ht;

	DataChunk &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);

	if (!ht) {
		ht = make_uniq<GroupedAggregateHashTable>(
		    context.client, BufferAllocator::Get(context.client), group_types, op.payload_types, op.bindings,
		    GroupedAggregateHashTable::InitialCapacity(), idx_t(RadixHTConfig::SINK_RADIX_BITS));
	}

	ht->AddChunk(group_chunk, payload_input, filter);

	if (AbandonHT(context.client, *ht)) {
		CombineInternal(context, input.global_state, input.local_state);
		ht->ClearPointerTable();
	}
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate_p) const {
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		return;
	}

	lstate.ht->Finalize();
	CombineInternal(context, gstate_p, lstate_p);
	lstate.ht.reset();
}

void RadixPartitionedHashTable::CombineInternal(ExecutionContext &, GlobalSinkState &gstate_p,
                                                LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();

	// Get data from the HT
	unique_ptr<PartitionedTupleData> partitioned_data;
	shared_ptr<ArenaAllocator> allocator;
	lstate.ht->GetDataOwnership(partitioned_data, allocator);

	auto &partitions = partitioned_data->GetPartitions();
	D_ASSERT(partitions.size() == gstate.sink_partitions.size());
	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		auto &partition = partitions[partition_idx];
		if (partition->Count() == 0) {
			continue;
		}
		auto &sink_partition = *gstate.sink_partitions[partition_idx];
		lock_guard<mutex> guard(sink_partition.lock);
		sink_partition.uncombined_data.emplace_back(std::move(partition));
		sink_partition.uncombined_data.back().allocators.emplace_back(allocator);
	}
}

bool RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();

	// Check if we want to repartition
	const auto requires_repartitioning = RequiresRepartitioning(context, gstate_p);
	D_ASSERT(gstate.finalize_radix_bits.IsValid());
	D_ASSERT(gstate.repartition_tasks_per_partition.IsValid());

	if (requires_repartitioning) { // Schedule repartition / finalize tasks
		D_ASSERT(gstate.finalize_radix_bits.GetIndex() > RadixHTConfig::SINK_RADIX_BITS);

		// Initialize global state
		const auto num_sink_partitions = RadixPartitioning::NumberOfPartitions(RadixHTConfig::SINK_RADIX_BITS);
		D_ASSERT(gstate.sink_partitions.size() == num_sink_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_sink_partitions; partition_idx++) {
			auto &sink_partition = gstate.sink_partitions[partition_idx];
			const auto num_data = sink_partition->uncombined_data.size();
			const auto tasks_per_partition = gstate.repartition_tasks_per_partition.GetIndex();
			sink_partition->data_per_repartition_task = (num_data + tasks_per_partition - 1) / tasks_per_partition;
			D_ASSERT(sink_partition->data_per_repartition_task.GetIndex() *
			             gstate.repartition_tasks_per_partition.GetIndex() >=
			         sink_partition->uncombined_data.size());
		}

		const auto num_finalize_partitions =
		    RadixPartitioning::NumberOfPartitions(gstate.finalize_radix_bits.GetIndex());
		const auto multiplier = num_finalize_partitions / num_sink_partitions;
		gstate.finalize_partitions.resize(num_finalize_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_finalize_partitions; partition_idx++) {
			auto &finalize_partition = gstate.finalize_partitions[partition_idx];
			finalize_partition = make_uniq<AggregatePartition>();

			// Estimate the count in the finalize partition
			const auto &original_partition = gstate.sink_partitions[partition_idx / multiplier];
			finalize_partition->count = original_partition->count.GetIndex() / multiplier;
		}
	} else { // No repartitioning necessary
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(RadixHTConfig::SINK_RADIX_BITS);
		gstate.finalize_partitions.resize(num_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			auto &finalize_partition = gstate.finalize_partitions[partition_idx];
			finalize_partition = std::move(gstate.sink_partitions[partition_idx]);
			finalize_partition->finalize_available = true;
		}
		gstate.sink_partitions.clear();
	}
	// TODO: function shouldn't return a bool anymore, there's never tasks
	return false;
}

void RadixPartitionedHashTable::ScheduleTasks(Executor &executor, const shared_ptr<Event> &event,
                                              GlobalSinkState &gstate_p, vector<shared_ptr<Task>> &tasks) const {
	// TODO: remove this NOP
}

bool RadixPartitionedHashTable::RequiresRepartitioning(ClientContext &context, GlobalSinkState &gstate_p) {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(RadixHTConfig::SINK_RADIX_BITS);
	D_ASSERT(gstate.sink_partitions.size() == num_partitions);

	// Get partition counts and sizes
	vector<idx_t> partition_counts(num_partitions, 0);
	vector<idx_t> partition_sizes(num_partitions, 0);
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		const auto &sink_partition = gstate.sink_partitions[partition_idx];
		for (auto &uncombined_data : sink_partition->uncombined_data) {
			partition_counts[partition_idx] += uncombined_data.data_collection->Count();
			partition_sizes[partition_idx] += uncombined_data.data_collection->SizeInBytes();
		}
	}

	// Find max partition size and total size
	idx_t total_count = 0;
	idx_t max_partition_idx = 0;
	idx_t max_partition_size = 0;
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		const auto &partition_count = partition_counts[partition_idx];
		const auto &partition_size = partition_sizes[partition_idx];
		auto partition_ht_size = partition_size + GroupedAggregateHashTable::PointerTableSize(partition_count);
		if (partition_ht_size > max_partition_size) {
			max_partition_idx = partition_idx;
			max_partition_size = partition_ht_size;
		}
		total_count += partition_count;
		// Also set the count in the partition
		gstate.sink_partitions[partition_idx]->count = partition_count;
	}

	// Switch to out-of-core finalize at ~60%
	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	const auto max_ht_size = double(0.6) * BufferManager::GetBufferManager(context).GetMaxMemory();

	// Possibly repartition based on total count
	auto finalize_tasks = NextPowerOfTwo(total_count / RadixHTConfig::FINALIZE_MAX_GROUP_COUNT);
	// Number of partitions has to be equal to or higher than current number of partitions
	finalize_tasks = MaxValue<idx_t>(finalize_tasks, num_partitions);
	// But not higher than the set max
	finalize_tasks =
	    MinValue<idx_t>(finalize_tasks, RadixPartitioning::NumberOfPartitions(RadixHTConfig::FINALIZE_MAX_RADIX_BITS));
	// Unless we have more threads than that
	finalize_tasks = MaxValue<idx_t>(finalize_tasks, NextPowerOfTwo(n_threads));

	// Largest partition count/size
	const auto partition_count = partition_counts[max_partition_idx];
	const auto partition_size = MaxValue<idx_t>(partition_sizes[max_partition_idx], 1);

	// Now we check if this number of partitions is already good enough to go out-of-core
	const auto max_added_bits = RadixPartitioning::MAX_RADIX_BITS - RadixHTConfig::SINK_RADIX_BITS;
	idx_t added_bits = RadixPartitioning::RadixBits(finalize_tasks) - RadixHTConfig::SINK_RADIX_BITS;
	if (context.config.force_external) {
		// Repartition to at least 2 more radix bits when forcing external
		added_bits = MinValue<idx_t>(added_bits, 2);
	}

	// Loop until we can surely fit the partitions in memory
	for (; added_bits < max_added_bits; added_bits++) {
		double partition_multiplier = RadixPartitioning::NumberOfPartitions(added_bits);

		auto new_estimated_count = double(partition_count) / partition_multiplier;
		auto new_estimated_size = double(partition_size) / partition_multiplier;
		auto new_estimated_ht_size =
		    new_estimated_size + GroupedAggregateHashTable::PointerTableSize(new_estimated_count);

		if (context.config.force_external || new_estimated_ht_size <= max_ht_size / n_threads) {
			break;
		}
	}

	gstate.finalize_radix_bits = RadixHTConfig::SINK_RADIX_BITS + added_bits;
	if (partition_size > max_ht_size) {
		// Single partition is very large, all threads work on same partition
		gstate.repartition_tasks_per_partition = finalize_tasks;
	} else {
		// Multiple partitions fit in memory, so multiple are repartitioned at the same time
		const auto partitions_in_memory = MinValue<idx_t>(max_ht_size / partition_size, num_partitions);
		gstate.repartition_tasks_per_partition =
		    MaxValue<idx_t>(NextPowerOfTwo(2 * n_threads) / partitions_in_memory, 1);
	}

	// Return true if we increased the radix bits
	D_ASSERT(gstate.finalize_radix_bits.GetIndex() >= RadixHTConfig::SINK_RADIX_BITS);
	return gstate.finalize_radix_bits.GetIndex() != RadixHTConfig::SINK_RADIX_BITS;
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
	if (!sink.final_count.IsValid()) {
		idx_t total_count = 0;
		for (auto &data : sink.final_data) {
			total_count += data.data_collection->Count();
		}
		lock_guard<mutex> guard(sink.lock);
		sink.final_count = total_count;
	}
	return sink.final_count.GetIndex();
}

void RadixPartitionedHashTable::SetMultiScan(GlobalSinkState &sink_p) {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	sink.scan_pin_properties = TupleDataPinProperties::UNPIN_AFTER_DONE;
}

enum class RadixHTSourceTaskType : uint8_t { REPARTITION, FINALIZE, SCAN, NO_TASK };

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
	mutex lock;
	atomic<bool> finished;

	//! For synchronizing repartition tasks
	idx_t repartition_idx;
	idx_t repartition_task_idx;

	//! For synchronizing finalize tasks
	idx_t finalize_idx;
	idx_t finalize_done;

	//! For synchronizing scan tasks
	idx_t scan_idx;
	idx_t scan_done;
};

enum class RadixHTScanStatus : uint8_t { INIT, IN_PROGRESS, DONE };

class RadixHTLocalSourceState : public LocalSourceState {
public:
	explicit RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Do the work this thread has been assigned
	void ExecuteTask(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished();

private:
	//! Execute the repartition, finalize, or scan task
	void Repartition(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate);
	void Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate);
	void Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk);

public:
	//! Assigned task and index
	RadixHTSourceTaskType task;
	optional_idx task_idx;
	//! Current status of the scan
	RadixHTScanStatus scan_status;

private:
	const RadixPartitionedHashTable &radix_ht;

	//! Thread-local HT that is re-used
	unique_ptr<GroupedAggregateHashTable> ht;

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
    : context(context_p), finished(false), repartition_idx(0), repartition_task_idx(0), finalize_idx(0),
      finalize_done(0), scan_idx(0), scan_done(0) {
	for (column_t column_id = 0; column_id < radix_ht.group_types.size(); column_id++) {
		column_ids.push_back(column_id);
	}
}

bool RadixHTGlobalSourceState::AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate) {
	lock_guard<mutex> guard(lock);

	// Try to assign a scan task first if we enabled multi-scanning,
	// if not multi-scanning, threads just immediately scan the HTs they finished
	if (sink.scan_pin_properties == TupleDataPinProperties::UNPIN_AFTER_DONE && scan_idx < sink.final_data.size()) {
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.task_idx = scan_idx++;
		lstate.scan_status = RadixHTScanStatus::INIT;
		return true;
	}

	// If not multi-scanning, try to assign a finalize task first
	if (finalize_idx < sink.finalize_partitions.size()) {
		auto &finalize_partition = *sink.finalize_partitions[finalize_idx];
		if (finalize_partition.finalize_available) {
			lstate.task = RadixHTSourceTaskType::FINALIZE;
			lstate.task_idx = finalize_idx++;
			finalize_partition.finalize_available = false;
			return true;
		}
	}

	// Finally, try to assign a repartition task
	if (repartition_idx < sink.sink_partitions.size()) {
		D_ASSERT(repartition_task_idx < sink.repartition_tasks_per_partition.GetIndex());
		lstate.task = RadixHTSourceTaskType::REPARTITION;
		lstate.task_idx = repartition_idx;
		if (++repartition_task_idx == sink.repartition_tasks_per_partition.GetIndex()) {
			repartition_idx++;
			repartition_task_idx = 0;
		}
		return true;
	}

	finished = true;
	return false;
}

RadixHTLocalSourceState::RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht_p)
    : task(RadixHTSourceTaskType::NO_TASK), radix_ht(radix_ht_p),
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
	case RadixHTSourceTaskType::REPARTITION:
		Repartition(sink, gstate);
		break;
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

void RadixHTLocalSourceState::Repartition(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate) {
	D_ASSERT(task == RadixHTSourceTaskType::REPARTITION);

	const auto num_sink_partitions = RadixPartitioning::NumberOfPartitions(RadixHTConfig::SINK_RADIX_BITS);
	const auto num_finalize_partitions = RadixPartitioning::NumberOfPartitions(sink.finalize_radix_bits.GetIndex());
	const auto multiplier = num_finalize_partitions / num_sink_partitions;

	const auto sink_partition_idx = task_idx.GetIndex();
	auto &sink_partition = *sink.sink_partitions[sink_partition_idx];

	// Acquire data
	vector<MaterializedAggregateData> uncombined_data;
	{
		lock_guard<mutex> guard(sink_partition.lock);
		while (!sink_partition.uncombined_data.empty() &&
		       uncombined_data.size() < sink_partition.data_per_repartition_task.GetIndex()) {
			uncombined_data.push_back(std::move(sink_partition.uncombined_data.back()));
			sink_partition.uncombined_data.pop_back();
		}
		D_ASSERT(sink_partition.uncombined_data.empty() ||
		         uncombined_data.size() == sink_partition.data_per_repartition_task.GetIndex());
	}

	if (!uncombined_data.empty()) {
		// Repartition the data
		auto &sink_data_collection = *uncombined_data[0].data_collection;
		for (idx_t i = 1; i < uncombined_data.size(); i++) {
			sink_data_collection.Combine(*uncombined_data[i].data_collection);
		}

		auto repartitioned_data = make_uniq<RadixPartitionedTupleData>(
		    BufferManager::GetBufferManager(gstate.context), sink_data_collection.GetLayout(),
		    sink.finalize_radix_bits.GetIndex(), layout.ColumnCount() - 1);
		repartitioned_data->Partition(sink_data_collection);

		// Add it to the finalize partitions
		auto &repartitioned_data_collections = repartitioned_data->GetPartitions();
		for (idx_t i = 0; i < multiplier; i++) {
			const auto finalize_partition_idx = sink_partition_idx * multiplier + i;
			auto &finalize_partition = *sink.finalize_partitions[finalize_partition_idx];
			auto &finalize_data_collection = repartitioned_data_collections[finalize_partition_idx];
			if (finalize_data_collection->Count() == 0) {
				continue;
			}

			lock_guard<mutex> guard(finalize_partition.lock);
			finalize_partition.uncombined_data.emplace_back(std::move(finalize_data_collection));
			auto &data = finalize_partition.uncombined_data.back();

			// Also give it ownership of the corresponding allocators
			for (auto &ucb : uncombined_data) {
				D_ASSERT(ucb.allocators.size() == 1);
				data.allocators.emplace_back(ucb.allocators[0]);
			}
		}
		uncombined_data.clear();
	}

	if (++sink_partition.repartition_tasks_done == sink.repartition_tasks_per_partition.GetIndex()) {
		// All repartition tasks are done, mark the finalizes as available
		sink_partition.uncombined_data.clear();
		for (idx_t i = 0; i < multiplier; i++) {
			const auto finalize_partition_idx = sink_partition_idx * multiplier + i;
			auto &finalize_partition = *sink.finalize_partitions[finalize_partition_idx];
			finalize_partition.finalize_available = true;
		}
	}
}

void RadixHTLocalSourceState::Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate) {
	D_ASSERT(task == RadixHTSourceTaskType::FINALIZE);

	auto &finalize_partition = *sink.finalize_partitions[task_idx.GetIndex()];
	D_ASSERT(!finalize_partition.finalize_available);

	if (!ht) {
		const auto count = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, finalize_partition.count.GetIndex());
		const auto capacity = NextPowerOfTwo(count * GroupedAggregateHashTable::LOAD_FACTOR);
		ht = make_uniq<GroupedAggregateHashTable>(gstate.context, BufferAllocator::Get(gstate.context),
		                                          radix_ht.group_types, radix_ht.op.payload_types, radix_ht.op.bindings,
		                                          capacity);
	} else {
		ht->ClearPointerTable();
	}

	auto &uncombined_data = finalize_partition.uncombined_data;
	if (uncombined_data.empty()) {
		return;
	}

	// Create one TupleDataCollection from all uncombined data in this partition
	auto &data_collection = *uncombined_data[0].data_collection;
	for (idx_t i = 1; i < uncombined_data.size(); i++) {
		data_collection.Combine(*uncombined_data[i].data_collection);
	}

	// Now combine
	ht->Combine(data_collection);

	idx_t scan_idx;
	{
		lock_guard<mutex> guard(gstate.lock);
		gstate.scan_idx = sink.AddToFinal(*ht, uncombined_data);
		scan_idx = gstate.scan_idx++;
		gstate.finalize_done++;
	}
	uncombined_data.clear();

	// This thread can now self-assign scanning the HT it just finalized
	task = RadixHTSourceTaskType::SCAN;
	task_idx = scan_idx;
	scan_status = RadixHTScanStatus::INIT;
}

void RadixHTLocalSourceState::Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk) {
	D_ASSERT(task == RadixHTSourceTaskType::SCAN);

	auto &data = sink.final_data[task_idx.GetIndex()];
	auto &data_collection = *data.data_collection;
	D_ASSERT(data_collection.Count() != 0);
	if (scan_status == RadixHTScanStatus::INIT) {
		data_collection.InitializeScan(scan_state, gstate.column_ids, sink.scan_pin_properties);
		scan_status = RadixHTScanStatus::IN_PROGRESS;
	}

	if (!data_collection.Scan(scan_state, scan_chunk)) {
		scan_status = RadixHTScanStatus::DONE;
		lock_guard<mutex> guard(gstate.lock);
		if (++gstate.scan_done == sink.final_data.size() && gstate.finalize_done == sink.finalize_partitions.size()) {
			gstate.finished = true;
		}
		return;
	}

	if (layout.GetTypes().empty()) {
		layout = data_collection.GetLayout().Copy();
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
	case RadixHTSourceTaskType::REPARTITION:
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

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
