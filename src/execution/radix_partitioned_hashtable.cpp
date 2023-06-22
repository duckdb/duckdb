#include "duckdb/execution/radix_partitioned_hashtable.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

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
struct UncombinedAggregateData {
	explicit UncombinedAggregateData(unique_ptr<TupleDataCollection> data_collection_p)
	    : data_collection(std::move(data_collection_p)) {
	}
	unique_ptr<TupleDataCollection> data_collection;
	vector<shared_ptr<ArenaAllocator>> allocators;
};

struct AggregatePartition {
	AggregatePartition() : repartition_tasks_assigned(0), repartition_tasks_done(0) {
	}

	mutex lock;
	vector<UncombinedAggregateData> uncombined_data;
	unique_ptr<GroupedAggregateHashTable> ht;

	optional_idx data_per_repartition_task;
	atomic<idx_t> repartition_tasks_assigned {0};
	atomic<idx_t> repartition_tasks_done {0};
};

class RadixHTGlobalState : public GlobalSinkState {
public:
	explicit RadixHTGlobalState(const RadixPartitionedHashTable &ht_p) : ht(ht_p) {
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(SINK_RADIX_BITS);
		sink_partitions.resize(num_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			sink_partitions[partition_idx] = make_uniq<AggregatePartition>();
		}
	}

	//! The HT object
	const RadixPartitionedHashTable &ht;

	//! Radix bits used during the Sink
	constexpr const static idx_t SINK_RADIX_BITS = 4;
	//! The radix partitions during the sink
	vector<unique_ptr<AggregatePartition>> sink_partitions;

	//! Radix bits used during the sink
	optional_idx finalize_radix_bits;
	//! Number of tasks per partition when repartitioning
	optional_idx tasks_per_partition;
	//! The radix partitions during the finalize
	vector<unique_ptr<AggregatePartition>> finalize_partitions;

	//! Lock for final stuff
	mutex lock;
	//! The final data collection containing all finalized data
	unique_ptr<TupleDataCollection> final_data_collection;
	//! The aggregate allocators for the final data collection
	vector<shared_ptr<ArenaAllocator>> final_allocators;
};

class RadixHTLocalState : public LocalSinkState {
public:
	explicit RadixHTLocalState(const RadixPartitionedHashTable &ht) {
		// if there are no groups we create a fake group so everything has the same group
		group_chunk.InitializeEmpty(ht.group_types);
		if (ht.grouping_set.empty()) {
			group_chunk.data[0].Reference(Value::TINYINT(42));
		}
	}

	//! Chunk with group columns
	DataChunk group_chunk;
	//! Append state
	AggregateHTAppendState append_state;
	//! The aggregate HT
	unique_ptr<GroupedAggregateHashTable> ht;
};

unique_ptr<GlobalSinkState> RadixPartitionedHashTable::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RadixHTGlobalState>(*this);
}

unique_ptr<LocalSinkState> RadixPartitionedHashTable::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RadixHTLocalState>(*this);
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
	auto &gstate = input.global_state.Cast<RadixHTGlobalState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalState>();

	DataChunk &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);

	// if we have non-combinable aggregates (e.g. string_agg) we cannot keep parallel hash tables
	if (ForceSingleHT(input.global_state)) {
		D_ASSERT(gstate.sink_partitions.size() == 1);
		auto &single_partition = *gstate.sink_partitions[0];
		lock_guard<mutex> glock(single_partition.lock);
		if (!single_partition.ht) {
			// Create a finalized ht in the global state, that we can populate
			single_partition.ht = make_uniq<GroupedAggregateHashTable>(
			    context.client, BufferAllocator::Get(context.client), group_types, op.payload_types, op.bindings);
		}
		single_partition.ht->AddChunk(lstate.append_state, group_chunk, payload_input, filter);
		return;
	}

	if (!lstate.ht) {
		lstate.ht = make_uniq<GroupedAggregateHashTable>(context.client, BufferAllocator::Get(context.client),
		                                                 group_types, op.payload_types, op.bindings);
	}
	lstate.ht->AddChunk(lstate.append_state, group_chunk, payload_input, filter);

	if (lstate.ht->Count() + STANDARD_VECTOR_SIZE > GroupedAggregateHashTable::SinkCapacity()) {
		CombineInternal(context, input.global_state, input.local_state);
		// TODO: "refresh" local HT by resetting first part and creating a new allocator
	}
}

void RadixPartitionedHashTable::Combine(ExecutionContext &context, GlobalSinkState &gstate_p,
                                        LocalSinkState &lstate_p) const {
	auto &lstate = lstate_p.Cast<RadixHTLocalState>();
	if (!lstate.ht) {
		return;
	}

	CombineInternal(context, gstate_p, lstate_p);
	lstate.ht->Finalize();
}

void RadixPartitionedHashTable::CombineInternal(ExecutionContext &context, GlobalSinkState &gstate_p,
                                                LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalState>();
	auto &lstate = lstate_p.Cast<RadixHTLocalState>();
	if (ForceSingleHT(gstate)) {
		D_ASSERT(gstate.sink_partitions.size() == 1);
		return;
	}

	for (idx_t partition_idx = 0; partition_idx < gstate.sink_partitions.size(); partition_idx++) {
		auto &partition = *gstate.sink_partitions[partition_idx];
		lock_guard<mutex> guard(partition.lock);
		// TODO get TDC and allocator, add to partition
	}
}

bool RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalState>();

	// Special case if we have non-combinable aggregates, for which we've already created a global shared HT
	if (ForceSingleHT(gstate)) {
		D_ASSERT(gstate.sink_partitions.size() == 1);
		// TODO: move data to final TDC
		return false; // No tasks needed
	}

	return true; // Tasks needed
}

// this task is run in multiple threads and combines the radix-partitioned hash tables into a single one and then
// folds them into the global ht finally.
class RadixAggregateFinalizeTask : public ExecutorTask {
public:
	RadixAggregateFinalizeTask(Executor &executor, shared_ptr<Event> event_p, RadixHTGlobalState &state_p,
	                           idx_t partition_idx_p)
	    : ExecutorTask(executor), event(std::move(event_p)), state(state_p), partition_idx(partition_idx_p) {
	}

	static void FinalizeHT(ClientContext &context, RadixHTGlobalState &gstate, idx_t partition_idx) {
		auto &finalize_partition = gstate.finalize_partitions[partition_idx];
		D_ASSERT(finalize_partition);

		if (!finalize_partition->ht) {
			lock_guard<mutex> guard(finalize_partition->lock);
			if (finalize_partition->ht) {
				return; // Another thread has started finalizing this
			}
			finalize_partition->ht =
			    make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), gstate.ht.group_types,
			                                         gstate.ht.op.payload_types, gstate.ht.op.bindings);
		}
		D_ASSERT(!finalize_partition->ht);

		if (finalize_partition->uncombined_data.empty()) {
			return;
		}

		// Create one TupleDataCollection from all uncombined data in this partition
		auto &uncombined_data = finalize_partition->uncombined_data;
		auto &data_collection = *uncombined_data[0].data_collection;
		for (idx_t i = 1; i < uncombined_data.size(); i++) {
			data_collection.Combine(*uncombined_data[1].data_collection);
		}

		// Now combine / finalize
		auto &ht = *finalize_partition->ht;
		ht.Combine(data_collection);
		ht.Finalize();

		// TODO: move data from finalize_partition to final_data_collection
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		FinalizeHT(executor.context, state, partition_idx);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	RadixHTGlobalState &state;
	idx_t partition_idx;
};

class RadixAggregateRepartitionTask : public ExecutorTask {
public:
	RadixAggregateRepartitionTask(Executor &executor, shared_ptr<Event> event_p, RadixHTGlobalState &gstate_p)
	    : ExecutorTask(executor), event(std::move(event_p)), gstate(gstate_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		D_ASSERT(gstate.tasks_per_partition.IsValid());
		const auto num_sink_partitions = RadixPartitioning::NumberOfPartitions(RadixHTGlobalState::SINK_RADIX_BITS);
		D_ASSERT(gstate.sink_partitions.size() == num_sink_partitions);
		const auto num_finalize_partitions =
		    RadixPartitioning::NumberOfPartitions(gstate.finalize_radix_bits.GetIndex());
		D_ASSERT(gstate.finalize_partitions.size() == num_finalize_partitions);
		D_ASSERT(num_finalize_partitions > num_sink_partitions);
		const auto multiplier = num_finalize_partitions / num_sink_partitions;

		idx_t sink_partition_idx = 0;
		idx_t finalize_partition_idx = 0;
		while (sink_partition_idx < gstate.sink_partitions.size() &&
		       finalize_partition_idx < gstate.finalize_partitions.size()) {

			// Loop over sink partitions until we find one that we can repartition
			for (; sink_partition_idx < num_sink_partitions; sink_partition_idx++) {
				auto &sink_partition = *gstate.sink_partitions[sink_partition_idx];
				D_ASSERT(sink_partition.data_per_repartition_task.IsValid());

				if (++sink_partition.repartition_tasks_assigned > gstate.tasks_per_partition.GetIndex()) {
					continue;
				}

				// Acquire data
				vector<UncombinedAggregateData> uncombined_data;
				if (!sink_partition.uncombined_data.empty()) {
					lock_guard<mutex> guard(sink_partition.lock);
					while (!sink_partition.uncombined_data.empty() &&
					       uncombined_data.size() < sink_partition.data_per_repartition_task.GetIndex()) {
						uncombined_data.push_back(std::move(sink_partition.uncombined_data.back()));
						uncombined_data.pop_back();
					}
				}

				if (!uncombined_data.empty()) {
					// Repartition the data
					auto &sink_data_collection = *uncombined_data[0].data_collection;
					for (idx_t i = 1; i < uncombined_data.size(); i++) {
						sink_data_collection.Combine(*uncombined_data[1].data_collection);
					}
					auto &layout = sink_data_collection.GetLayout();
					auto repartitioned_data = make_uniq<RadixPartitionedTupleData>(
					    BufferManager::GetBufferManager(executor.context), layout,
					    gstate.finalize_radix_bits.GetIndex(), layout.ColumnCount() - 1);
					repartitioned_data->Partition(sink_data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE);

					// Add it to the finalize partitions
					auto &repartitioned_data_collections = repartitioned_data->GetPartitions();
					for (idx_t i = 0; i < multiplier; i++) {
						const auto partition_idx = sink_partition_idx * multiplier + i;
						auto &finalize_partition = *gstate.finalize_partitions[partition_idx];
						auto &finalize_data_collection = repartitioned_data_collections[partition_idx];

						lock_guard<mutex> guard(finalize_partition.lock);
						finalize_partition.uncombined_data.emplace_back(std::move(finalize_data_collection));
						auto &data = finalize_partition.uncombined_data.back();

						// Also give it ownership of the corresponding allocators
						for (auto &ucb : uncombined_data) {
							D_ASSERT(ucb.allocators.size() == 1);
							data.allocators.emplace_back(std::move(ucb.allocators[0]));
						}
					}
					uncombined_data.clear();
				}

				if (++sink_partition.repartition_tasks_done == gstate.tasks_per_partition.GetIndex()) {
					sink_partition.uncombined_data.clear();
					sink_partition.ht.reset();
				}
				break;
			}

			// Loop over repartitioned partitions
			for (; finalize_partition_idx < num_finalize_partitions; finalize_partition_idx++) {
				const auto original_radix = finalize_partition_idx / multiplier;
				auto &sink_partition = gstate.sink_partitions[original_radix];

				if (sink_partition->repartition_tasks_done < gstate.tasks_per_partition.GetIndex()) {
					break; // Needs more repartitioning
				}

				// We can finalize!
				RadixAggregateFinalizeTask::FinalizeHT(executor.context, gstate, finalize_partition_idx);
			}
		}

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	RadixHTGlobalState &gstate;
};

void RadixPartitionedHashTable::ScheduleTasks(Executor &executor, const shared_ptr<Event> &event,
                                              GlobalSinkState &gstate_p, vector<shared_ptr<Task>> &tasks) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalState>();
	D_ASSERT(!ForceSingleHT(gstate_p));

	// Check if we want to repartition
	auto requires_repartitioning = RequiresRepartitioning(executor.context, gstate_p);
	D_ASSERT(gstate.finalize_radix_bits.IsValid());
	D_ASSERT(gstate.tasks_per_partition.IsValid());

	if (requires_repartitioning) { // Schedule repartition / finalize tasks
		D_ASSERT(gstate.finalize_radix_bits.GetIndex() > RadixHTGlobalState::SINK_RADIX_BITS);

		// Initialize global state
		const auto num_sink_partitions = RadixPartitioning::NumberOfPartitions(RadixHTGlobalState::SINK_RADIX_BITS);
		D_ASSERT(gstate.sink_partitions.size() == num_sink_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_sink_partitions; partition_idx++) {
			auto &sink_partition = gstate.sink_partitions[partition_idx];
			sink_partition->data_per_repartition_task =
			    MaxValue<idx_t>(sink_partition->uncombined_data.size() / gstate.tasks_per_partition.GetIndex(), 1);
		}

		const auto num_finalize_partitions =
		    RadixPartitioning::NumberOfPartitions(gstate.finalize_radix_bits.GetIndex());
		gstate.finalize_partitions.resize(num_finalize_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_finalize_partitions; partition_idx++) {
			gstate.finalize_partitions[partition_idx] = make_uniq<AggregatePartition>();
		}

		// Schedule tasks equal to number of threads
		const idx_t num_threads = TaskScheduler::GetScheduler(executor.context).NumberOfThreads();
		for (idx_t i = 0; i < num_threads; i++) {
			tasks.emplace_back(make_shared<RadixAggregateRepartitionTask>(executor, event, gstate));
		}
	} else { // No repartitioning necessary
		const auto num_partitions = RadixPartitioning::NumberOfPartitions(RadixHTGlobalState::SINK_RADIX_BITS);
		gstate.finalize_partitions.resize(num_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			gstate.finalize_partitions[partition_idx] = std::move(gstate.sink_partitions[partition_idx]);
			tasks.push_back(make_uniq<RadixAggregateFinalizeTask>(executor, event, gstate, partition_idx));
		}
		gstate.sink_partitions.clear();
	}
}

bool RadixPartitionedHashTable::ForceSingleHT(GlobalSinkState &state) {
	// TODO
	//	auto &gstate = state.Cast<RadixHTGlobalState>();
	//	return gstate.partition_info->n_partitions < 2;
}

bool RadixPartitionedHashTable::RequiresRepartitioning(ClientContext &context, GlobalSinkState &gstate_p) {
	auto &gstate = gstate_p.Cast<RadixHTGlobalState>();
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(RadixHTGlobalState::SINK_RADIX_BITS);
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
	idx_t total_size = 0;
	idx_t max_partition_idx = 0;
	idx_t max_partition_size = 0;
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		const auto &partition_count = partition_counts[partition_idx];
		const auto &partition_size = partition_sizes[partition_idx];
		auto partition_ht_size = partition_size + GroupedAggregateHashTable::FirstPartSize(partition_count);
		if (partition_ht_size > max_partition_size) {
			max_partition_idx = partition_idx;
			max_partition_size = partition_ht_size;
		}
		total_size += partition_ht_size;
	}

	// Switch to out-of-core finalize at ~60%
	const auto max_ht_size = double(0.6) * BufferManager::GetBufferManager(context).GetMaxMemory();
	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	// Desired number of tasks is at least 2x the number of threads
	const auto desired_tasks = NextPowerOfTwo(2 * n_threads);
	if (!context.config.force_external && total_size < max_ht_size) {
		// In-memory finalize
		if (num_partitions >= desired_tasks) { // Can already keep all threads busy
			gstate.finalize_radix_bits = RadixHTGlobalState::SINK_RADIX_BITS;
			gstate.tasks_per_partition = 1;
			return false;
		} else { // LCOV_EXCL_START
			// Can't have coverage because we always have more partitions than threads on github actions
			gstate.finalize_radix_bits = RadixPartitioning::RadixBits(desired_tasks);
			gstate.tasks_per_partition = desired_tasks / n_threads;
			return true;
		} // LCOV_EXCL_START
	}

	// Out-of-core finalize
	const auto partition_count = partition_counts[max_partition_idx];
	const auto partition_size = partition_sizes[max_partition_idx];

	const auto max_added_bits = RadixPartitioning::MAX_RADIX_BITS - RadixHTGlobalState::SINK_RADIX_BITS;
	idx_t added_bits = context.config.force_external ? 2 : 1;
	for (; added_bits < max_added_bits; added_bits++) {
		double partition_multiplier = RadixPartitioning::NumberOfPartitions(added_bits);

		auto new_estimated_count = double(partition_count) / partition_multiplier;
		auto new_estimated_size = double(partition_size) / partition_multiplier;
		auto new_estimated_ht_size = new_estimated_size + GroupedAggregateHashTable::FirstPartSize(new_estimated_count);

		if (context.config.force_external || new_estimated_ht_size <= max_ht_size / n_threads / 4) {
			// Aim for an estimated partition size of max_ht_size / 4
			break;
		}
	}

	gstate.finalize_radix_bits = RadixHTGlobalState::SINK_RADIX_BITS + added_bits;
	if (partition_size > max_ht_size) {
		// Single partition is very large, all threads work on same partition
		gstate.tasks_per_partition = desired_tasks;
	} else {
		// Multiple partitions fit in memory, threads work on multiple at a time
		const auto partitions_in_memory = max_ht_size / partition_size;
		gstate.tasks_per_partition = desired_tasks / partitions_in_memory;
	}

	return true;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class RadixHTGlobalSourceState : public GlobalSourceState {
public:
	explicit RadixHTGlobalSourceState(const RadixPartitionedHashTable &ht) : initialized(false), finished(false) {
	}

	void Initialize(TupleDataCollection &data_collection) {
		lock_guard<mutex> guard(global_scan_state.lock);
		if (initialized) {
			return;
		}
		auto &layout = data_collection.GetLayout();
		vector<column_t> column_ids;
		column_ids.reserve(layout.ColumnCount() - 1);
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount() - 1; col_idx++) {
			column_ids.emplace_back(col_idx);
		}
		data_collection.InitializeScan(global_scan_state, column_ids);
	}

	//! All data and scan state
	TupleDataParallelScanState global_scan_state;
	atomic<bool> initialized;
	atomic<bool> finished;
};

class RadixHTLocalSourceState : public LocalSourceState {
public:
	explicit RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &ht) {
		auto &allocator = BufferAllocator::Get(context.client);
		auto scan_chunk_types = ht.group_types;
		for (auto &aggr_type : ht.op.aggregate_return_types) {
			scan_chunk_types.push_back(aggr_type);
		}
		scan_chunk.Initialize(allocator, scan_chunk_types);
	}

	//! Materialized GROUP BY expressions & aggregates
	DataChunk scan_chunk;
	//! Scan state for the current HT
	TupleDataLocalScanState scan_state;
};

idx_t RadixPartitionedHashTable::Count(duckdb::GlobalSinkState &sink_p) const {
	auto &sink = sink_p.Cast<RadixHTGlobalState>();
	const auto count = sink.final_data_collection->Count();
	return count == 0 && grouping_set.empty() ? 1 : count;
}

unique_ptr<GlobalSourceState> RadixPartitionedHashTable::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<RadixHTGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> RadixPartitionedHashTable::GetLocalSourceState(ExecutionContext &context) const {
	return make_uniq<RadixHTLocalSourceState>(context, *this);
}

SourceResultType RadixPartitionedHashTable::GetData(ExecutionContext &context, DataChunk &chunk,
                                                    GlobalSinkState &sink_p, OperatorSourceInput &input) const {
	auto &sink = sink_p.Cast<RadixHTGlobalState>();
	auto &gstate = input.global_state.Cast<RadixHTGlobalSourceState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSourceState>();
	if (gstate.finished) {
		return SourceResultType::FINISHED;
	}

	// special case hack to sort out aggregating from empty intermediates for aggregations without groups
	const auto count = sink.final_data_collection->Count();
	if (count == 0 && grouping_set.empty()) {
		D_ASSERT(chunk.ColumnCount() == null_groups.size() + op.aggregates.size() + op.grouping_functions.size());
		// for each column in the aggregates, set to initial state
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
		return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
	}

	if (count == 0) {
		gstate.finished = true;
		return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
	}

	lstate.scan_chunk.Reset();
	if (!gstate.initialized) {
		gstate.Initialize(*sink.final_data_collection);
	}

	auto &local_scan_state = lstate.scan_state;
	if (!sink.final_data_collection->Scan(gstate.global_scan_state, local_scan_state, lstate.scan_chunk)) {
		gstate.finished = true;
		return SourceResultType::FINISHED;
	}

	idx_t chunk_index = 0;
	for (auto &entry : grouping_set) {
		chunk.data[entry].Reference(lstate.scan_chunk.data[chunk_index++]);
	}
	for (auto null_group : null_groups) {
		chunk.data[null_group].SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(chunk.data[null_group], true);
	}
	D_ASSERT(grouping_set.size() + null_groups.size() == op.GroupCount());
	for (idx_t col_idx = 0; col_idx < op.aggregates.size(); col_idx++) {
		chunk.data[op.GroupCount() + col_idx].Reference(lstate.scan_chunk.data[group_types.size() + col_idx]);
	}
	D_ASSERT(op.grouping_functions.size() == grouping_values.size());
	for (idx_t i = 0; i < op.grouping_functions.size(); i++) {
		chunk.data[op.GroupCount() + op.aggregates.size() + i].Reference(grouping_values[i]);
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
