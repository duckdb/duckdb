#include "duckdb/execution/radix_partitioned_hashtable.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
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
	static constexpr const idx_t SINK_ABANDON_THRESHOLD = 100000;
	//! If we cross SINK_ABANDON_THRESHOLD, we decide whether to continue with the current HT or abandon it.
	//! Abandoning is better if the input has virtually no duplicates.
	//! Continuing is better if there are a significant amount of duplicates.
	//! If 524288 tuples went into our current HT, and there are 510583 uniques, do we abandon?
	//! Seems like we should, but if our input is random uniform, we are exactly on track to see 10.000.000 groups.
	//! If our input size is 100.000.000, then we see each tuple 10 times, and abandoning is actually a bad choice.
	//! All of this is to defend against our greatest enemy, the random uniform distribution with repetition.
	//! We keep track of the size of the HT, and the number of tuples that went into it.
	//! If we are on track to see 25x our current unique count, we can safely abandon the HTs early!
	//! (TODO maybe this whole calculation is a bit overcomplicated, we can just try to combine during the sink)
	static constexpr const idx_t SINK_EXPECTED_GROUP_COUNT_FACTOR = 25;
	//! Combine abandoned data after crossing this threshold
	static constexpr const idx_t SINK_COMBINE_THRESHOLD = 100000;
	//! Radix bits used during the finalize (if more than SINK_RADIX_BITS are needed)
	static constexpr const idx_t FINALIZE_RADIX_BITS = 8;

	//! Utility functions
	static constexpr idx_t SinkPartitionCount() {
		return RadixPartitioning::NumberOfPartitions(SINK_RADIX_BITS);
	}
	static constexpr idx_t FinalizePartitionCount() {
		return RadixPartitioning::NumberOfPartitions(FINALIZE_RADIX_BITS);
	}
	static constexpr idx_t PartitionMultiplier() {
		static_assert(FinalizePartitionCount() > SinkPartitionCount(),
		              "Finalize partition count must be greater than sink partition count");
		return FinalizePartitionCount() / SinkPartitionCount();
	}
};

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

unique_ptr<GroupedAggregateHashTable> RadixPartitionedHashTable::CreateHT(ClientContext &context, const idx_t capacity,
                                                                          const idx_t radix_bits) const {
	return make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), group_types, op.payload_types,
	                                            op.bindings, capacity, radix_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct AggregatePartition {
	AggregatePartition()
	    : uncombined_count(0), combine_active(false), repartition_tasks_done(0), finalize_available(false) {
	}

	void AddUncombinedData(MaterializedAggregateData &&ucb) {
		uncombined_count += ucb.data_collection->Count();
		uncombined_data.emplace_back(std::move(ucb));
	}

	vector<MaterializedAggregateData> AcquireUncombinedData() {
		auto result = std::move(uncombined_data);
		uncombined_count = 0;
		uncombined_data = vector<MaterializedAggregateData>();
		return result;
	}

	//! Uncombined data from the thread-local HTs is appended here
	mutex uncombined_data_lock;
	vector<MaterializedAggregateData> uncombined_data;
	idx_t uncombined_count;

	//! HT that combines the data
	mutex combined_data_lock;
	atomic<bool> combine_active;
	unique_ptr<GroupedAggregateHashTable> combined_data;
	vector<shared_ptr<ArenaAllocator>> combined_data_allocators;

	//! For synchronizing repartitioning
	optional_idx data_per_repartition_task;
	atomic<idx_t> repartition_tasks_done;

	//! For synchronizing finalizing
	atomic<bool> finalize_available;
};

class RadixHTGlobalSinkState : public GlobalSinkState {
public:
	RadixHTGlobalSinkState();

	void CombinedToFinalize(const idx_t sink_partition_idx);

	//! Adds to final data to be scanned
	void AddToFinal(GroupedAggregateHashTable &intermediate_ht, vector<MaterializedAggregateData> &uncombined_data);
	void AddToFinal(MaterializedAggregateData &&uncombined_data);

	//! Destroys aggregate states (if multi-scan)
	void Destroy();
	~RadixHTGlobalSinkState() override;

public:
	mutex lock;
	//! Whether we've called Finalize
	bool finalized;
	//! Whether we've repartitioned
	bool repartitioned;

	//! The radix partitions during the sink
	vector<unique_ptr<AggregatePartition>> sink_partitions;

	//! Whether any data has been added to the finalize partitions
	atomic<bool> finalize_in_use;
	//! Radix bits used during the sink
	optional_idx finalize_radix_bits;
	//! Number of tasks per partition when repartitioning
	optional_idx repartition_tasks_per_partition;
	//! The radix partitions during the finalize
	vector<unique_ptr<AggregatePartition>> finalize_partitions;

	//! For synchronizing repartition tasks
	idx_t repartition_idx;
	idx_t repartition_task_idx;
	optional_idx repartition_tasks;
	atomic<idx_t> repartition_done;

	//! For synchronizing finalize tasks
	idx_t finalize_idx;
	optional_idx finalize_tasks;
	atomic<idx_t> finalize_done;

	//! Pin properties when scanning
	TupleDataPinProperties scan_pin_properties;
	//! The final data that has to be scanned
	vector<MaterializedAggregateData> scan_data;
	//! Total count before combining
	idx_t count_before_combining;
};

RadixHTGlobalSinkState::RadixHTGlobalSinkState()
    : finalized(false), repartitioned(false), finalize_in_use(false), repartition_idx(0), repartition_task_idx(0),
      repartition_done(0), finalize_idx(0), finalize_done(0),
      scan_pin_properties(TupleDataPinProperties::DESTROY_AFTER_DONE), count_before_combining(0) {
	// Initialize sink partitions
	sink_partitions.resize(RadixHTConfig::SinkPartitionCount());
	for (idx_t partition_idx = 0; partition_idx < RadixHTConfig::SinkPartitionCount(); partition_idx++) {
		sink_partitions[partition_idx] = make_uniq<AggregatePartition>();
	}
	// Initialize finalize partitions
	finalize_partitions.resize(RadixHTConfig::FinalizePartitionCount());
	for (idx_t partition_idx = 0; partition_idx < RadixHTConfig::FinalizePartitionCount(); partition_idx++) {
		finalize_partitions[partition_idx] = make_uniq<AggregatePartition>();
	}
}

void RadixHTGlobalSinkState::CombinedToFinalize(const idx_t sink_partition_idx) {
	auto &sink_partition = *sink_partitions[sink_partition_idx];
	auto repartitioned_data = sink_partition.combined_data->AcquireData();
	D_ASSERT(repartitioned_data.size() == RadixHTConfig::FinalizePartitionCount());
	for (idx_t i = 0; i < RadixHTConfig::PartitionMultiplier(); i++) {
		const auto finalize_partition_idx = sink_partition_idx * RadixHTConfig::PartitionMultiplier() + i;
		auto &finalize_partition = *finalize_partitions[finalize_partition_idx];

		lock_guard<mutex> finalize_guard(finalize_partition.uncombined_data_lock);
		finalize_partition.AddUncombinedData(std::move(repartitioned_data[finalize_partition_idx]));
		for (auto &allocator : sink_partition.combined_data_allocators) {
			finalize_partition.combined_data_allocators.emplace_back(allocator);
		}
	}
	sink_partition.combined_data_allocators.clear();
	finalize_in_use = true;
}

void RadixHTGlobalSinkState::AddToFinal(GroupedAggregateHashTable &intermediate_ht,
                                        vector<MaterializedAggregateData> &uncombined_data) {
	auto data = intermediate_ht.AcquireData(true);
	D_ASSERT(data.size() == 1);
	D_ASSERT(data[0].data_collection->Count() != 0);
	lock_guard<mutex> guard(lock);
	scan_data.emplace_back(std::move(data[0]));
	for (auto &ucb : uncombined_data) {
		D_ASSERT(!ucb.allocators.empty());
		for (auto &allocator : ucb.allocators) {
			scan_data.back().allocators.emplace_back(allocator);
		}
	}
}

void RadixHTGlobalSinkState::AddToFinal(MaterializedAggregateData &&data) {
	D_ASSERT(data.data_collection->Count() != 0);
	lock_guard<mutex> guard(lock);
	scan_data.emplace_back(std::move(data));
}

void RadixHTGlobalSinkState::Destroy() {
	if (scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE) {
		return;
	}

	for (auto &data : scan_data) {
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

RadixHTGlobalSinkState::~RadixHTGlobalSinkState() {
	Destroy();
}

class RadixHTLocalSinkState : public LocalSinkState {
public:
	explicit RadixHTLocalSinkState(const RadixPartitionedHashTable &ht);

public:
	//! Thread-local HT that is re-used
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Chunk with group columns
	DataChunk group_chunk;
};

RadixHTLocalSinkState::RadixHTLocalSinkState(const RadixPartitionedHashTable &ht) {
	// if there are no groups we create a fake group so everything has the same group
	group_chunk.InitializeEmpty(ht.group_types);
	if (ht.grouping_set.empty()) {
		group_chunk.data[0].Reference(Value::TINYINT(42));
	}
}

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &) const {
	return make_uniq<RadixHTGlobalSinkState>();
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

struct RadixHTAbandonStatus {
	//! Whether we should abandon the HT
	bool abandon = false;
	//! Whether we're over the per-HT memory limit
	bool over_memory_limit = false;
	//! Whether there are too many uniques in the HT
	bool too_many_uniques = false;
	//! Whether we're abandoning a HT due to calling RadixPartitionedHashTable::Combine
	bool combine = false;
};

static idx_t SingleHTMemoryLimit(ClientContext &context) {
	auto hts_in_memory = TaskScheduler::GetScheduler(context).NumberOfThreads() + RadixHTConfig::SinkPartitionCount();
	const auto limit = double(0.6) * BufferManager::GetBufferManager(context).GetMaxMemory() / hts_in_memory;
	if (context.config.force_external) {
		return MinValue<double>(Storage::BLOCK_SIZE * RadixHTConfig::SinkPartitionCount() * 1.5, limit);
	}
	return limit;
}

static RadixHTAbandonStatus GetAbandonStatus(ClientContext &context, const GroupedAggregateHashTable &ht) {
	RadixHTAbandonStatus result;
	if (ht.TotalSize() > SingleHTMemoryLimit(context)) {
		result.abandon = true;
		result.over_memory_limit = true;
	}

	if (TaskScheduler::GetScheduler(context).NumberOfThreads() == 1) {
		// Single thread, no need to abandon
		return result;
	}

	if (result.abandon || ht.Count() > RadixHTConfig::SINK_ABANDON_THRESHOLD) {
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

		if (ht.Count() > threshold) {
			result.abandon = true;
			result.too_many_uniques = true;
		}
	}
	return result;
}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                     DataChunk &payload_input, const unsafe_vector<idx_t> &filter) const {
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();

	auto &ht = lstate.ht;
	if (!ht) {
		ht = CreateHT(context.client, GroupedAggregateHashTable::InitialCapacity(), RadixHTConfig::SINK_RADIX_BITS);
	}

	auto &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);
	ht->AddChunk(group_chunk, payload_input, filter);

	const auto local_status = GetAbandonStatus(context.client, *ht);
	if (local_status.abandon) {
		CombineInternal(context.client, input.global_state, input.local_state, local_status);
		ht->ClearPointerTable();
	}
}

void RadixPartitionedHashTable::CombineInternal(ClientContext &context, GlobalSinkState &gstate_p,
                                                LocalSinkState &lstate_p,
                                                const RadixHTAbandonStatus &local_status) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	D_ASSERT(local_status.abandon);

	// Get data from the thread-local HT
	auto partitioned_data = lstate.ht->AcquireData();
	D_ASSERT(partitioned_data.size() == gstate.sink_partitions.size());

	for (idx_t partition_idx = 0; partition_idx < partitioned_data.size(); partition_idx++) {
		auto &partition_data = partitioned_data[partition_idx];
		if (partition_data.data_collection->Count() == 0) {
			continue;
		}

		// Add the data to the thread-global state
		auto &partition = *gstate.sink_partitions[partition_idx];
		vector<MaterializedAggregateData> uncombined_data;
		{
			lock_guard<mutex> guard(partition.uncombined_data_lock);
			partition.AddUncombinedData(std::move(partition_data));

			if (ShouldCombine(context, gstate_p, partition_idx, local_status)) {
				// Acquire all uncombined data from the partition
				partition.combine_active = true;
				uncombined_data = partition.AcquireUncombinedData();
			}
		}

		if (!uncombined_data.empty()) {
			CombinePartition(context, gstate_p, partition_idx, uncombined_data, local_status);
		}
	}
}

bool RadixPartitionedHashTable::ShouldCombine(ClientContext &context, GlobalSinkState &gstate_p,
                                              const idx_t sink_partition_idx,
                                              const RadixHTAbandonStatus &local_status) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	auto &sink_partition = *gstate.sink_partitions[sink_partition_idx];

	if (local_status.combine) {
		// We abandoned the local HT due to calling Combine, we don't actually combine here, leave it for later
		return false;
	}

	if (sink_partition.uncombined_count < RadixHTConfig::SINK_COMBINE_THRESHOLD) {
		// We haven't reached the threshold yet
		return false;
	}

	if (sink_partition.combine_active) {
		// There's already a combine active
		return false;
	}

	lock_guard<mutex> sink_guard(sink_partition.combined_data_lock);
	if (!sink_partition.combined_data) {
		// We haven't combined any data yet for this partition, so let's do that
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(sink_partition.uncombined_count);
		sink_partition.combined_data = CreateHT(context, capacity, RadixHTConfig::FINALIZE_RADIX_BITS);
		return true;
	}

	// We've combined data before, check whether we should abandon that
	const auto global_status = GetAbandonStatus(context, *sink_partition.combined_data);
	if (!global_status.abandon) {
		// No reason to abandon, just combine into it
		return true;
	}

	if (global_status.too_many_uniques) {
		// Too many uniques in the global HT
		if (local_status.too_many_uniques) {
			// Very unlikely that we'll be able to reduce data size, leave combining for later
			return false;
		}
		// We can probably reduce data size
		if (global_status.over_memory_limit) {
			// Combined HT has grown too large, unpin it before combining
			gstate.CombinedToFinalize(sink_partition_idx);
			sink_partition.combined_data->ClearPointerTable();
		}
	}
	return true;
}

void RadixPartitionedHashTable::CombinePartition(ClientContext &context, GlobalSinkState &gstate_p,
                                                 const idx_t sink_partition_idx,
                                                 vector<MaterializedAggregateData> &uncombined_data,
                                                 const RadixHTAbandonStatus &local_status) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	D_ASSERT(!uncombined_data.empty());
	D_ASSERT(!local_status.combine);

	auto &partition = *gstate.sink_partitions[sink_partition_idx];
	D_ASSERT(partition.combine_active);
	lock_guard<mutex> combined_guard(partition.combined_data_lock);
	D_ASSERT(partition.combined_data);

	while (!uncombined_data.empty()) {
		auto ucb = std::move(uncombined_data.back());
		D_ASSERT(ucb.allocators.size() == 1);
		uncombined_data.pop_back();

		partition.combined_data->Combine(*ucb.data_collection);
		partition.combined_data_allocators.emplace_back(std::move(ucb.allocators[0]));

		const auto global_status = GetAbandonStatus(context, *partition.combined_data);
		if (!global_status.abandon) {
			continue;
		}

		// We have to abandon the global HT
		if (global_status.too_many_uniques && local_status.too_many_uniques) {
			// Very unlikely that we'll be able to reduce data size, just leave the data here
			break;
		}

		// We can probably reduce data size, but we need to abandon the global HT
		gstate.CombinedToFinalize(sink_partition_idx);
		partition.combined_data->ClearPointerTable();
	}

	if (!uncombined_data.empty()) {
		// We bailed out on combining everything, move data back to partition
		lock_guard<mutex> uncombined_guard(partition.uncombined_data_lock);
		for (auto &ucb : uncombined_data) {
			partition.AddUncombinedData(std::move(ucb));
		}
	}

	partition.combine_active = false;
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate_p) const {
	auto &lstate = lstate_p.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		return;
	}

	RadixHTAbandonStatus local_status;
	local_status.abandon = true;
	local_status.combine = true;

	lstate.ht->Finalize();
	CombineInternal(context.client, gstate_p, lstate_p, local_status);
	lstate.ht.reset();
}

void RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();
	D_ASSERT(gstate.sink_partitions.size() == RadixHTConfig::SinkPartitionCount());
	D_ASSERT(gstate.finalize_partitions.size() == RadixHTConfig::FinalizePartitionCount());

	// Compute total count of uncombined data
	for (auto &sink_partition : gstate.sink_partitions) {
		gstate.count_before_combining += sink_partition->uncombined_count;
		gstate.count_before_combining += sink_partition->combined_data ? sink_partition->combined_data->Count() : 0;
	}
	for (auto &finalize_partition : gstate.finalize_partitions) {
		gstate.count_before_combining += finalize_partition->uncombined_count;
		gstate.count_before_combining +=
		    finalize_partition->combined_data ? finalize_partition->combined_data->Count() : 0;
	}

	// Check if we want to repartition
	if (RequiresRepartitioning(context, gstate_p)) {
		gstate.repartitioned = true;
		// Move the combined data in the sink partitions to the finalize partitions
		for (idx_t partition_idx = 0; partition_idx < RadixHTConfig::SinkPartitionCount(); partition_idx++) {
			auto &sink_partition = gstate.sink_partitions[partition_idx];
			if (sink_partition->combined_data) {
				gstate.CombinedToFinalize(partition_idx);
			}

			// Create repartition tasks for the uncombined data
			const auto num_data = sink_partition->uncombined_data.size();
			const auto tasks_per_partition = gstate.repartition_tasks_per_partition.GetIndex();
			sink_partition->data_per_repartition_task = (num_data + tasks_per_partition - 1) / tasks_per_partition;
			D_ASSERT(sink_partition->data_per_repartition_task.GetIndex() *
			             gstate.repartition_tasks_per_partition.GetIndex() >=
			         sink_partition->uncombined_data.size());
		}

		// Estimate the count in the finalize partitions
		for (idx_t partition_idx = 0; partition_idx < RadixHTConfig::FinalizePartitionCount(); partition_idx++) {
			auto &finalize_partition = *gstate.finalize_partitions[partition_idx];
			const auto &original_partition =
			    *gstate.sink_partitions[partition_idx / RadixHTConfig::PartitionMultiplier()];
			finalize_partition.uncombined_count +=
			    original_partition.uncombined_count / RadixHTConfig::PartitionMultiplier();
		}
	} else {
		D_ASSERT(!gstate.finalize_in_use);
		gstate.finalize_partitions.clear();
		gstate.finalize_partitions.resize(RadixHTConfig::SinkPartitionCount());

		for (idx_t partition_idx = 0; partition_idx < RadixHTConfig::SinkPartitionCount(); partition_idx++) {
			auto &finalize_partition = gstate.finalize_partitions[partition_idx];
			finalize_partition = std::move(gstate.sink_partitions[partition_idx]);
			finalize_partition->finalize_available = true;
		}
		gstate.sink_partitions.clear();
	}
	gstate.repartition_tasks = gstate.sink_partitions.size();
	gstate.finalize_tasks = gstate.finalize_partitions.size();
	gstate.finalized = true;
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
		partition_counts[partition_idx] = sink_partition->uncombined_count;
		partition_counts[partition_idx] += sink_partition->combined_data ? sink_partition->combined_data->Count() : 0;
		for (auto &uncombined_data : sink_partition->uncombined_data) {
			partition_sizes[partition_idx] += uncombined_data.data_collection->SizeInBytes();
		}
	}

	// Find max partition size and total size
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
	}

	// Switch to out-of-core finalize at ~60%
	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	const auto max_ht_size = double(0.6) * BufferManager::GetBufferManager(context).GetMaxMemory();

	// Largest partition count/size
	const auto partition_count = partition_counts[max_partition_idx];
	const auto partition_size = MaxValue<idx_t>(partition_sizes[max_partition_idx], 1);
	const auto partition_ht_size = partition_size + GroupedAggregateHashTable::PointerTableSize(partition_count);

	if (context.config.force_external || gstate.finalize_in_use || n_threads > RadixHTConfig::SinkPartitionCount() ||
	    n_threads * partition_ht_size > max_ht_size) {
		gstate.finalize_radix_bits = RadixHTConfig::FINALIZE_RADIX_BITS;
	} else {
		gstate.finalize_radix_bits = RadixHTConfig::SINK_RADIX_BITS;
	}

	if (context.config.force_external || partition_size > max_ht_size) {
		// Single partition is very large, all threads work on same partition
		gstate.repartition_tasks_per_partition = n_threads;
	} else {
		// Multiple partitions fit in memory, so multiple are repartitioned at the same time
		const auto partitions_in_memory = MinValue<idx_t>(max_ht_size / partition_size, num_partitions);
		gstate.repartition_tasks_per_partition = (n_threads + partitions_in_memory - 1) / partitions_in_memory;
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
	return sink.count_before_combining;
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
	atomic<bool> finished;

	//! For synchronizing scan tasks
	idx_t scan_idx;
	atomic<idx_t> scan_done;
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
	if (scan_done == sink.scan_data.size() && sink.finalize_done == sink.finalize_tasks.GetIndex()) {
		finished = true;
	}
	if (finished) {
		return false;
	}

	// Try to assign a scan task first
	if (scan_idx < sink.scan_data.size()) {
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.task_idx = scan_idx++;
		lstate.scan_collection = sink.scan_data[lstate.task_idx.GetIndex()].data_collection.get();
		lstate.scan_status = RadixHTScanStatus::INIT;
		return true;
	}

	// Try to assign a finalize task next
	if (sink.finalize_idx < sink.finalize_tasks.GetIndex()) {
		auto &finalize_partition = *sink.finalize_partitions[sink.finalize_idx];
		if (finalize_partition.finalize_available) {
			lstate.task = RadixHTSourceTaskType::FINALIZE;
			lstate.task_idx = sink.finalize_idx++;
			finalize_partition.finalize_available = false;
			return true;
		}
	}

	// Finally, try to assign a repartition task
	if (sink.repartition_idx < sink.repartition_tasks.GetIndex()) {
		D_ASSERT(sink.repartition_task_idx < sink.repartition_tasks_per_partition.GetIndex());
		lstate.task = RadixHTSourceTaskType::REPARTITION;
		lstate.task_idx = sink.repartition_idx;
		if (++sink.repartition_task_idx == sink.repartition_tasks_per_partition.GetIndex()) {
			sink.repartition_idx++;
			sink.repartition_task_idx = 0;
		}
		return true;
	}

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
	D_ASSERT(scan_status != RadixHTScanStatus::IN_PROGRESS);

	const auto num_sink_partitions = RadixPartitioning::NumberOfPartitions(RadixHTConfig::SINK_RADIX_BITS);
	const auto num_finalize_partitions = RadixPartitioning::NumberOfPartitions(sink.finalize_radix_bits.GetIndex());
	const auto multiplier = num_finalize_partitions / num_sink_partitions;

	const auto sink_partition_idx = task_idx.GetIndex();
	auto &sink_partition = *sink.sink_partitions[sink_partition_idx];

	// Acquire data
	vector<MaterializedAggregateData> uncombined_data;
	{
		lock_guard<mutex> guard(sink_partition.uncombined_data_lock);
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

		auto &data_layout = sink_data_collection.GetLayout();
		auto repartitioned_data =
		    make_uniq<RadixPartitionedTupleData>(BufferManager::GetBufferManager(gstate.context), data_layout,
		                                         sink.finalize_radix_bits.GetIndex(), data_layout.ColumnCount() - 1);
		repartitioned_data->Partition(sink_data_collection);

		// Add it to the finalize partitions
		lock_guard<mutex> guard(sink.lock);
		auto &repartitioned_data_collections = repartitioned_data->GetPartitions();
		for (idx_t i = 0; i < multiplier; i++) {
			const auto finalize_partition_idx = sink_partition_idx * multiplier + i;
			auto &finalize_partition = *sink.finalize_partitions[finalize_partition_idx];
			auto &finalize_data_collection = repartitioned_data_collections[finalize_partition_idx];
			if (finalize_data_collection->Count() == 0) {
				continue;
			}

			finalize_partition.uncombined_data.emplace_back(
			    MaterializedAggregateData(std::move(finalize_data_collection)));
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
		// All repartition tasks for this partition are done, mark the finalizes as available
		D_ASSERT(sink_partition.uncombined_data.empty());
		for (idx_t i = 0; i < multiplier; i++) {
			const auto finalize_partition_idx = sink_partition_idx * multiplier + i;
			auto &finalize_partition = *sink.finalize_partitions[finalize_partition_idx];
			finalize_partition.finalize_available = true;
		}
		sink.repartition_done++;
	}
}

void RadixHTLocalSourceState::Finalize(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate) {
	D_ASSERT(task == RadixHTSourceTaskType::FINALIZE);
	D_ASSERT(scan_status != RadixHTScanStatus::IN_PROGRESS);

	auto &finalize_partition = *sink.finalize_partitions[task_idx.GetIndex()];
	D_ASSERT(!finalize_partition.finalize_available);

	if (!sink.repartitioned && !finalize_partition.combined_data && finalize_partition.uncombined_data.size() == 1) {
		// Special case, no need to combine if there's only one uncombined data
		sink.AddToFinal(std::move(finalize_partition.uncombined_data[0]));
		sink.finalize_done++;
		return;
	}

	if (finalize_partition.combined_data) {
		ht = std::move(finalize_partition.combined_data);
		// Resize existing HT to sufficient capacity
		const auto capacity =
		    GroupedAggregateHashTable::GetCapacityForCount(ht->Count() + finalize_partition.uncombined_count);
		if (ht->Capacity() < capacity) {
			ht->Resize(capacity);
		}
	} else if (!ht) {
		// Create a HT with sufficient capacity
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(finalize_partition.uncombined_count);
		ht = radix_ht.CreateHT(gstate.context, capacity, 0);
	} else {
		ht->ClearPointerTable();
	}

	auto &uncombined_data = finalize_partition.uncombined_data;
	if (!uncombined_data.empty()) {
		// Create one TupleDataCollection from all uncombined data in this partition
		auto &data_collection = *uncombined_data[0].data_collection;
		for (idx_t i = 1; i < uncombined_data.size(); i++) {
			data_collection.Combine(*uncombined_data[i].data_collection);
		}

		// Now combine
		ht->Combine(data_collection);
		ht->UnpinData();

		// Add final data collection to global state
		sink.AddToFinal(*ht, uncombined_data);
		uncombined_data.clear();
	}

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
			auto &data = sink.scan_data[task_idx.GetIndex()];
			data.data_collection.reset();
			data.allocators.clear();
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
