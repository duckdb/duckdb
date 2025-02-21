#include "duckdb/execution/radix_partitioned_hashtable.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

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
				grouping_value += 1LL << (grouping.size() - (i + 1));
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
enum class AggregatePartitionState : uint8_t {
	//! Can be finalized
	READY_TO_FINALIZE = 0,
	//! Finalize is in progress
	FINALIZE_IN_PROGRESS = 1,
	//! Finalized, ready to scan
	READY_TO_SCAN = 2
};

struct AggregatePartition : StateWithBlockableTasks {
	explicit AggregatePartition(unique_ptr<TupleDataCollection> data_p)
	    : state(AggregatePartitionState::READY_TO_FINALIZE), data(std::move(data_p)), progress(0) {
	}

	AggregatePartitionState state;

	unique_ptr<TupleDataCollection> data;
	atomic<double> progress;
};

class RadixHTGlobalSinkState;

struct RadixHTConfig {
public:
	explicit RadixHTConfig(RadixHTGlobalSinkState &sink);

	void SetRadixBits(const idx_t &radix_bits_p);
	bool SetRadixBitsToExternal();
	idx_t GetRadixBits() const;
	idx_t GetMaximumSinkRadixBits() const;
	idx_t GetExternalRadixBits() const;

private:
	void SetRadixBitsInternal(idx_t radix_bits_p, bool external);
	idx_t InitialSinkRadixBits() const;
	idx_t MaximumSinkRadixBits() const;
	idx_t SinkCapacity() const;

private:
	//! The global sink state
	RadixHTGlobalSinkState &sink;

public:
	//! Number of threads (from TaskScheduler)
	const idx_t number_of_threads;
	//! Width of tuples
	const idx_t row_width;
	//! Capacity of HTs during the Sink
	const idx_t sink_capacity;

private:
	//! Assume (1 << 15) = 32KB L1 cache per core, divided by two because hyperthreading
	static constexpr idx_t L1_CACHE_SIZE = 32768 / 2;
	//! Assume (1 << 20) = 1MB L2 cache per core, divided by two because hyperthreading
	static constexpr idx_t L2_CACHE_SIZE = 1048576 / 2;
	//! Assume (1 << 20) + (1 << 19) = 1.5MB L3 cache per core (shared), divided by two because hyperthreading
	static constexpr idx_t L3_CACHE_SIZE = 1572864 / 2;

	//! Sink radix bits to initialize with
	static constexpr idx_t MAXIMUM_INITIAL_SINK_RADIX_BITS = 4;
	//! Maximum Sink radix bits (independent of threads)
	static constexpr idx_t MAXIMUM_FINAL_SINK_RADIX_BITS = 8;

	//! Current thread-global sink radix bits
	atomic<idx_t> sink_radix_bits;
	//! Maximum Sink radix bits (set based on number of threads)
	const idx_t maximum_sink_radix_bits;

	//! Thresholds at which we reduce the sink radix bits
	//! This needed to reduce cache misses when we have very wide rows
	static constexpr idx_t ROW_WIDTH_THRESHOLD_ONE = 32;
	static constexpr idx_t ROW_WIDTH_THRESHOLD_TWO = 64;

public:
	//! If we have this many or less threads, we grow the HT, otherwise we abandon
	static constexpr idx_t GROW_STRATEGY_THREAD_THRESHOLD = 2;
	//! If we fill this many blocks per partition, we trigger a repartition
	static constexpr double BLOCK_FILL_FACTOR = 1.8;
	//! By how many bits to repartition if a repartition is triggered
	static constexpr idx_t REPARTITION_RADIX_BITS = 2;
};

class RadixHTGlobalSinkState : public GlobalSinkState {
public:
	RadixHTGlobalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

	//! Destroys aggregate states (if multi-scan)
	~RadixHTGlobalSinkState() override;
	void Destroy();

public:
	ClientContext &context;
	//! Temporary memory state for managing this hash table's memory usage
	unique_ptr<TemporaryMemoryState> temporary_memory_state;
	atomic<idx_t> minimum_reservation;

	//! Whether we've called Finalize
	bool finalized;
	//! Whether we are doing an external aggregation
	atomic<bool> external;
	//! Threads that have called Sink
	atomic<idx_t> active_threads;
	//! Number of threads (from TaskScheduler)
	const idx_t number_of_threads;
	//! If any thread has called combine
	atomic<bool> any_combined;

	//! The radix HT
	const RadixPartitionedHashTable &radix_ht;
	//! Config for partitioning
	RadixHTConfig config;

	//! Uncombined partitioned data that will be put into the AggregatePartitions
	unique_ptr<PartitionedTupleData> uncombined_data;
	//! Allocators used during the Sink/Finalize
	vector<shared_ptr<ArenaAllocator>> stored_allocators;
	idx_t stored_allocators_size;

	//! Partitions that are finalized during GetData
	vector<unique_ptr<AggregatePartition>> partitions;
	//! For keeping track of progress
	atomic<idx_t> finalize_done;

	//! Pin properties when scanning
	TupleDataPinProperties scan_pin_properties;
	//! Total count before combining
	idx_t count_before_combining;
	//! Maximum partition size if all unique
	idx_t max_partition_size;
};

RadixHTGlobalSinkState::RadixHTGlobalSinkState(ClientContext &context_p, const RadixPartitionedHashTable &radix_ht_p)
    : context(context_p), temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)),
      finalized(false), external(false), active_threads(0),
      number_of_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
      any_combined(false), radix_ht(radix_ht_p), config(*this), stored_allocators_size(0), finalize_done(0),
      scan_pin_properties(TupleDataPinProperties::DESTROY_AFTER_DONE), count_before_combining(0),
      max_partition_size(0) {

	// Compute minimum reservation
	auto block_alloc_size = BufferManager::GetBufferManager(context).GetBlockAllocSize();
	auto tuples_per_block = block_alloc_size / radix_ht.GetLayout().GetRowWidth();
	idx_t ht_count =
	    LossyNumericCast<idx_t>(static_cast<double>(config.sink_capacity) / GroupedAggregateHashTable::LOAD_FACTOR);
	auto num_partitions = RadixPartitioning::NumberOfPartitions(config.GetMaximumSinkRadixBits());
	auto count_per_partition = ht_count / num_partitions;
	auto blocks_per_partition = (count_per_partition + tuples_per_block) / tuples_per_block;
	if (!radix_ht.GetLayout().AllConstant()) {
		blocks_per_partition += 1;
	}
	auto ht_size = num_partitions * blocks_per_partition * block_alloc_size + config.sink_capacity * sizeof(ht_entry_t);

	// This really is the minimum reservation that we can do
	auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	minimum_reservation = num_threads * ht_size;

	temporary_memory_state->SetMinimumReservation(minimum_reservation);
	temporary_memory_state->SetRemainingSizeAndUpdateReservation(context, minimum_reservation);
}

RadixHTGlobalSinkState::~RadixHTGlobalSinkState() {
	Destroy();
}

// LCOV_EXCL_START
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
	auto guard = Lock();
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
// LCOV_EXCL_STOP

RadixHTConfig::RadixHTConfig(RadixHTGlobalSinkState &sink_p)
    : sink(sink_p), number_of_threads(sink.number_of_threads), row_width(sink.radix_ht.GetLayout().GetRowWidth()),
      sink_capacity(SinkCapacity()), sink_radix_bits(InitialSinkRadixBits()),
      maximum_sink_radix_bits(MaximumSinkRadixBits()) {
}

void RadixHTConfig::SetRadixBits(const idx_t &radix_bits_p) {
	SetRadixBitsInternal(MinValue(radix_bits_p, maximum_sink_radix_bits), false);
}

bool RadixHTConfig::SetRadixBitsToExternal() {
	SetRadixBitsInternal(MAXIMUM_FINAL_SINK_RADIX_BITS, true);
	return sink.external;
}

idx_t RadixHTConfig::GetRadixBits() const {
	return sink_radix_bits;
}

idx_t RadixHTConfig::GetMaximumSinkRadixBits() const {
	return maximum_sink_radix_bits;
}

idx_t RadixHTConfig::GetExternalRadixBits() const {
	return MAXIMUM_FINAL_SINK_RADIX_BITS;
}

void RadixHTConfig::SetRadixBitsInternal(const idx_t radix_bits_p, bool external) {
	if (sink_radix_bits > radix_bits_p || sink.any_combined) {
		return;
	}

	auto guard = sink.Lock();
	if (sink_radix_bits > radix_bits_p || sink.any_combined) {
		return;
	}

	if (external) {
		const auto partition_multiplier = RadixPartitioning::NumberOfPartitions(radix_bits_p) /
		                                  RadixPartitioning::NumberOfPartitions(sink_radix_bits);
		sink.minimum_reservation = sink.minimum_reservation * partition_multiplier;
		sink.external = true;
	}

	sink_radix_bits = radix_bits_p;
}

idx_t RadixHTConfig::InitialSinkRadixBits() const {
	return MinValue(RadixPartitioning::RadixBitsOfPowerOfTwo(NextPowerOfTwo(number_of_threads)),
	                MAXIMUM_INITIAL_SINK_RADIX_BITS);
}

idx_t RadixHTConfig::MaximumSinkRadixBits() const {
	if (number_of_threads <= GROW_STRATEGY_THREAD_THRESHOLD) {
		return InitialSinkRadixBits(); // Don't repartition unless we go external
	}
	// If rows are very wide we have to reduce the number of partitions, otherwise cache misses get out of hand
	if (row_width >= ROW_WIDTH_THRESHOLD_TWO) {
		return MAXIMUM_FINAL_SINK_RADIX_BITS - 2;
	}
	if (row_width >= ROW_WIDTH_THRESHOLD_ONE) {
		return MAXIMUM_FINAL_SINK_RADIX_BITS - 1;
	}
	return MAXIMUM_FINAL_SINK_RADIX_BITS;
}

idx_t RadixHTConfig::SinkCapacity() const {
	// Compute cache size per active thread (assuming cache is shared)
	const auto total_shared_cache_size = number_of_threads * L3_CACHE_SIZE;
	const auto cache_per_active_thread = L1_CACHE_SIZE + L2_CACHE_SIZE + total_shared_cache_size / number_of_threads;

	// Divide cache per active thread by entry size, round up to next power of two, to get capacity
	const auto size_per_entry = LossyNumericCast<idx_t>(sizeof(ht_entry_t) * GroupedAggregateHashTable::LOAD_FACTOR) +
	                            MinValue(row_width, ROW_WIDTH_THRESHOLD_TWO);
	const auto capacity = NextPowerOfTwo(cache_per_active_thread / size_per_entry);

	// Capacity must be at least the minimum capacity
	return MaxValue<idx_t>(capacity, GroupedAggregateHashTable::InitialCapacity());
}

class RadixHTLocalSinkState : public LocalSinkState {
public:
	RadixHTLocalSinkState(ClientContext &context, const RadixPartitionedHashTable &radix_ht);

public:
	//! Thread-local HT that is re-used after abandoning
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Chunk with group columns
	DataChunk group_chunk;

	//! Data that is abandoned ends up here (only if we're doing external aggregation)
	unique_ptr<PartitionedTupleData> abandoned_data;
};

RadixHTLocalSinkState::RadixHTLocalSinkState(ClientContext &, const RadixPartitionedHashTable &radix_ht) {
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
		D_ASSERT(group->GetExpressionType() == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		// Reference from input_chunk[group.index] -> group_chunk[chunk_index]
		group_chunk.data[chunk_index++].Reference(input_chunk.data[bound_ref_expr.index]);
	}
	group_chunk.SetCardinality(input_chunk.size());
	group_chunk.Verify();
}

void MaybeRepartition(ClientContext &context, RadixHTGlobalSinkState &gstate, RadixHTLocalSinkState &lstate) {
	auto &config = gstate.config;
	auto &ht = *lstate.ht;

	// Check if we're approaching the memory limit
	auto &temporary_memory_state = *gstate.temporary_memory_state;
	const auto aggregate_allocator_size = ht.GetAggregateAllocator()->AllocationSize();
	const auto total_size =
	    aggregate_allocator_size + ht.GetPartitionedData().SizeInBytes() + ht.Capacity() * sizeof(ht_entry_t);
	idx_t thread_limit = temporary_memory_state.GetReservation() / gstate.number_of_threads;
	if (total_size > thread_limit) {
		// We're over the thread memory limit
		if (!gstate.external) {
			// We haven't yet triggered out-of-core behavior, but maybe we don't have to, grab the lock and check again
			auto guard = gstate.Lock();
			thread_limit = temporary_memory_state.GetReservation() / gstate.number_of_threads;
			if (total_size > thread_limit) {
				// Out-of-core would be triggered below, update minimum reservation and try to increase the reservation
				temporary_memory_state.SetMinimumReservation(aggregate_allocator_size * gstate.number_of_threads +
				                                             gstate.minimum_reservation);
				auto remaining_size =
				    MaxValue<idx_t>(gstate.number_of_threads * total_size, temporary_memory_state.GetRemainingSize());
				temporary_memory_state.SetRemainingSizeAndUpdateReservation(context, 2 * remaining_size);
				thread_limit = temporary_memory_state.GetReservation() / gstate.number_of_threads;
			}
		}
	}

	if (total_size > thread_limit) {
		if (gstate.config.SetRadixBitsToExternal()) {
			// We're approaching the memory limit, unpin the data
			if (!lstate.abandoned_data) {
				lstate.abandoned_data = make_uniq<RadixPartitionedTupleData>(
				    BufferManager::GetBufferManager(context), gstate.radix_ht.GetLayout(), config.GetRadixBits(),
				    gstate.radix_ht.GetLayout().ColumnCount() - 1);
			}
			ht.SetRadixBits(gstate.config.GetRadixBits());
			ht.AcquirePartitionedData()->Repartition(*lstate.abandoned_data);
		}
	}

	// We can go external when there are few threads, but we shouldn't repartition here
	if (gstate.number_of_threads <= RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD) {
		return;
	}

	const auto partition_count = ht.GetPartitionedData().PartitionCount();
	const auto current_radix_bits = RadixPartitioning::RadixBitsOfPowerOfTwo(partition_count);
	D_ASSERT(current_radix_bits <= config.GetRadixBits());

	const auto block_size = BufferManager::GetBufferManager(context).GetBlockSize();
	const auto row_size_per_partition =
	    ht.GetPartitionedData().Count() * ht.GetPartitionedData().GetLayout().GetRowWidth() / partition_count;
	if (row_size_per_partition > LossyNumericCast<idx_t>(config.BLOCK_FILL_FACTOR * static_cast<double>(block_size))) {
		// We crossed our block filling threshold, try to increment radix bits
		config.SetRadixBits(current_radix_bits + config.REPARTITION_RADIX_BITS);
	}

	const auto global_radix_bits = config.GetRadixBits();
	if (current_radix_bits == global_radix_bits) {
		return; // We're already on the right number of radix bits
	}

	// We're out-of-sync with the global radix bits, repartition
	ht.SetRadixBits(global_radix_bits);
	ht.Repartition();
}

void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                     DataChunk &payload_input, const unsafe_vector<idx_t> &filter) const {
	auto &gstate = input.global_state.Cast<RadixHTGlobalSinkState>();
	auto &lstate = input.local_state.Cast<RadixHTLocalSinkState>();
	if (!lstate.ht) {
		lstate.ht = CreateHT(context.client, gstate.config.sink_capacity, gstate.config.GetRadixBits());
		gstate.active_threads++;
	}

	auto &group_chunk = lstate.group_chunk;
	PopulateGroupChunk(group_chunk, chunk);

	auto &ht = *lstate.ht;
	ht.AddChunk(group_chunk, payload_input, filter);

	if (ht.Count() + STANDARD_VECTOR_SIZE < GroupedAggregateHashTable::ResizeThreshold(gstate.config.sink_capacity)) {
		return; // We can fit another chunk
	}

	if (gstate.number_of_threads > RadixHTConfig::GROW_STRATEGY_THREAD_THRESHOLD || gstate.external) {
		// 'Reset' the HT without taking its data, we can just keep appending to the same collection
		// This only works because we never resize the HT
		// We don't do this when running with 1 or 2 threads, it only makes sense when there's many threads
		ht.Abandon();

		// Once we've inserted more than SKIP_LOOKUP_THRESHOLD tuples,
		// and more than UNIQUE_PERCENTAGE_THRESHOLD were unique,
		// we set the HT to skip doing lookups, which makes it blindly append data to the HT.
		// This speeds up adding data, at the cost of no longer de-duplicating.
		// The data will be de-duplicated later anyway
		static constexpr idx_t SKIP_LOOKUP_THRESHOLD = 262144;
		static constexpr double UNIQUE_PERCENTAGE_THRESHOLD = 0.95;
		const auto unique_percentage =
		    static_cast<double>(ht.GetPartitionedData().Count()) / static_cast<double>(ht.GetSinkCount());
		if (ht.GetSinkCount() > SKIP_LOOKUP_THRESHOLD && unique_percentage > UNIQUE_PERCENTAGE_THRESHOLD) {
			ht.SkipLookups();
		}
	}

	// Check if we need to repartition
	const auto radix_bits_before = ht.GetRadixBits();
	MaybeRepartition(context.client, gstate, lstate);
	const auto repartitioned = radix_bits_before != ht.GetRadixBits();

	if (repartitioned && ht.Count() != 0) {
		// We repartitioned, but we didn't clear the pointer table / reset the count because we're on 1 or 2 threads
		ht.Abandon();
		if (gstate.external) {
			ht.Resize(gstate.config.sink_capacity);
		}
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

	// Set any_combined, then check one last time whether we need to repartition
	gstate.any_combined = true;
	MaybeRepartition(context.client, gstate, lstate);

	auto &ht = *lstate.ht;
	auto lstate_data = ht.AcquirePartitionedData();
	if (lstate.abandoned_data) {
		D_ASSERT(gstate.external);
		D_ASSERT(lstate.abandoned_data->PartitionCount() == lstate.ht->GetPartitionedData().PartitionCount());
		D_ASSERT(lstate.abandoned_data->PartitionCount() ==
		         RadixPartitioning::NumberOfPartitions(gstate.config.GetRadixBits()));
		lstate.abandoned_data->Combine(*lstate_data);
	} else {
		lstate.abandoned_data = std::move(lstate_data);
	}

	auto guard = gstate.Lock();
	if (gstate.uncombined_data) {
		gstate.uncombined_data->Combine(*lstate.abandoned_data);
	} else {
		gstate.uncombined_data = std::move(lstate.abandoned_data);
	}
	gstate.stored_allocators.emplace_back(ht.GetAggregateAllocator());
	gstate.stored_allocators_size += gstate.stored_allocators.back()->AllocationSize();
}

void RadixPartitionedHashTable::Finalize(ClientContext &context, GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<RadixHTGlobalSinkState>();

	if (gstate.uncombined_data) {
		auto &uncombined_data = *gstate.uncombined_data;
		gstate.count_before_combining = uncombined_data.Count();

		// If true there is no need to combine, it was all done by a single thread in a single HT
		const auto single_ht = !gstate.external && gstate.active_threads == 1 && gstate.number_of_threads == 1;

		auto &uncombined_partition_data = uncombined_data.GetPartitions();
		const auto n_partitions = uncombined_partition_data.size();
		gstate.partitions.reserve(n_partitions);
		for (idx_t i = 0; i < n_partitions; i++) {
			auto &partition = uncombined_partition_data[i];
			auto partition_size =
			    partition->SizeInBytes() +
			    GroupedAggregateHashTable::GetCapacityForCount(partition->Count()) * sizeof(ht_entry_t);
			gstate.max_partition_size = MaxValue(gstate.max_partition_size, partition_size);

			gstate.partitions.emplace_back(make_uniq<AggregatePartition>(std::move(partition)));
			if (single_ht) {
				gstate.finalize_done++;
				gstate.partitions.back()->progress = 1;
				gstate.partitions.back()->state = AggregatePartitionState::READY_TO_SCAN;
			}
		}
	} else {
		gstate.count_before_combining = 0;
	}

	// Minimum of combining one partition at a time
	gstate.temporary_memory_state->SetMinimumReservation(gstate.stored_allocators_size + gstate.max_partition_size);
	// Set size to 0 until the scan actually starts
	gstate.temporary_memory_state->SetZero();
	gstate.finalized = true;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
idx_t RadixPartitionedHashTable::MaxThreads(GlobalSinkState &sink_p) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	if (sink.partitions.empty()) {
		return 0;
	}

	const auto max_threads = MinValue<idx_t>(
	    NumericCast<idx_t>(TaskScheduler::GetScheduler(sink.context).NumberOfThreads()), sink.partitions.size());
	sink.temporary_memory_state->SetRemainingSizeAndUpdateReservation(
	    sink.context, sink.stored_allocators_size + max_threads * sink.max_partition_size);

	// we cannot spill aggregate state memory
	const auto usable_memory = sink.temporary_memory_state->GetReservation() > sink.stored_allocators_size
	                               ? sink.temporary_memory_state->GetReservation() - sink.stored_allocators_size
	                               : 0;
	// This many partitions will fit given our reservation (at least 1))
	const auto partitions_fit = MaxValue<idx_t>(usable_memory / sink.max_partition_size, 1);

	// Mininum of the two
	return MinValue<idx_t>(partitions_fit, max_threads);
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
	SourceResultType AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate,
	                            InterruptState &interrupt_state);

public:
	//! The client context
	ClientContext &context;
	//! For synchronizing the source phase
	atomic<bool> finished;

	//! Column ids for scanning
	vector<column_t> column_ids;

	//! For synchronizing tasks
	idx_t task_idx;
	atomic<idx_t> task_done;
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
	//! Current task and index
	RadixHTSourceTaskType task;
	idx_t task_idx;

	//! Thread-local HT that is re-used to Finalize
	unique_ptr<GroupedAggregateHashTable> ht;
	//! Current status of a Scan
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
    : context(context_p), finished(false), task_idx(0), task_done(0) {
	for (column_t column_id = 0; column_id < radix_ht.group_types.size(); column_id++) {
		column_ids.push_back(column_id);
	}
}

SourceResultType RadixHTGlobalSourceState::AssignTask(RadixHTGlobalSinkState &sink, RadixHTLocalSourceState &lstate,
                                                      InterruptState &interrupt_state) {
	// First, try to get a partition index
	auto guard = sink.Lock();
	if (finished || task_idx == sink.partitions.size()) {
		lstate.ht.reset();
		return SourceResultType::FINISHED;
	}
	lstate.task_idx = task_idx++;

	// We got a partition index
	auto &partition = *sink.partitions[lstate.task_idx];
	auto partition_guard = partition.Lock();
	switch (partition.state) {
	case AggregatePartitionState::READY_TO_FINALIZE:
		partition.state = AggregatePartitionState::FINALIZE_IN_PROGRESS;
		lstate.task = RadixHTSourceTaskType::FINALIZE;
		return SourceResultType::HAVE_MORE_OUTPUT;
	case AggregatePartitionState::FINALIZE_IN_PROGRESS:
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.scan_status = RadixHTScanStatus::INIT;
		return partition.BlockSource(partition_guard, interrupt_state);
	case AggregatePartitionState::READY_TO_SCAN:
		lstate.task = RadixHTSourceTaskType::SCAN;
		lstate.scan_status = RadixHTScanStatus::INIT;
		return SourceResultType::HAVE_MORE_OUTPUT;
	default:
		throw InternalException("Unexpected AggregatePartitionState in RadixHTLocalSourceState::Finalize!");
	}
}

RadixHTLocalSourceState::RadixHTLocalSourceState(ExecutionContext &context, const RadixPartitionedHashTable &radix_ht)
    : task(RadixHTSourceTaskType::NO_TASK), task_idx(DConstants::INVALID_INDEX), scan_status(RadixHTScanStatus::DONE),
      layout(radix_ht.GetLayout().Copy()), aggregate_allocator(BufferAllocator::Get(context.client)) {
	auto &allocator = BufferAllocator::Get(context.client);
	auto scan_chunk_types = radix_ht.group_types;
	for (auto &aggr_type : radix_ht.op.aggregate_return_types) {
		scan_chunk_types.push_back(aggr_type);
	}
	scan_chunk.Initialize(allocator, scan_chunk_types);
}

void RadixHTLocalSourceState::ExecuteTask(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate,
                                          DataChunk &chunk) {
	D_ASSERT(task != RadixHTSourceTaskType::NO_TASK);
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

	if (!ht) {
		// This capacity would always be sufficient for all data
		const auto capacity = GroupedAggregateHashTable::GetCapacityForCount(partition.data->Count());

		// However, we will limit the initial capacity so we don't do a huge over-allocation
		const auto n_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(gstate.context).NumberOfThreads());
		const auto memory_limit = BufferManager::GetBufferManager(gstate.context).GetMaxMemory();
		const idx_t thread_limit = LossyNumericCast<idx_t>(0.6 * double(memory_limit) / double(n_threads));

		const idx_t size_per_entry = partition.data->SizeInBytes() / MaxValue<idx_t>(partition.data->Count(), 1) +
		                             idx_t(GroupedAggregateHashTable::LOAD_FACTOR * sizeof(ht_entry_t));
		// but not lower than the initial capacity
		const auto capacity_limit =
		    MaxValue(NextPowerOfTwo(thread_limit / size_per_entry), GroupedAggregateHashTable::InitialCapacity());

		ht = sink.radix_ht.CreateHT(gstate.context, MinValue<idx_t>(capacity, capacity_limit), 0);
	} else {
		ht->Abandon();
	}

	// Now combine the uncombined data using this thread's HT
	ht->Combine(*partition.data, &partition.progress);
	partition.progress = 1;

	// Move the combined data back to the partition
	partition.data =
	    make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(gstate.context), sink.radix_ht.GetLayout());
	partition.data->Combine(*ht->AcquirePartitionedData()->GetPartitions()[0]);

	// Update thread-global state
	auto guard = sink.Lock();
	sink.stored_allocators.emplace_back(ht->GetAggregateAllocator());
	if (task_idx == sink.partitions.size()) {
		ht.reset();
	}
	const auto finalizes_done = ++sink.finalize_done;
	D_ASSERT(finalizes_done <= sink.partitions.size());
	if (finalizes_done == sink.partitions.size()) {
		// All finalizes are done, set remaining size to 0
		sink.temporary_memory_state->SetZero();
	}

	// Update partition state
	auto partition_guard = partition.Lock();
	partition.state = AggregatePartitionState::READY_TO_SCAN;
	partition.UnblockTasks(partition_guard);

	// This thread will scan the partition
	task = RadixHTSourceTaskType::SCAN;
	scan_status = RadixHTScanStatus::INIT;
}

void RadixHTLocalSourceState::Scan(RadixHTGlobalSinkState &sink, RadixHTGlobalSourceState &gstate, DataChunk &chunk) {
	D_ASSERT(task == RadixHTSourceTaskType::SCAN);
	D_ASSERT(scan_status != RadixHTScanStatus::DONE);

	auto &partition = *sink.partitions[task_idx];
	D_ASSERT(partition.state == AggregatePartitionState::READY_TO_SCAN);
	auto &data_collection = *partition.data;

	if (scan_status == RadixHTScanStatus::INIT) {
		data_collection.InitializeScan(scan_state, gstate.column_ids, sink.scan_pin_properties);
		scan_status = RadixHTScanStatus::IN_PROGRESS;
	}

	if (!data_collection.Scan(scan_state, scan_chunk)) {
		if (sink.scan_pin_properties == TupleDataPinProperties::DESTROY_AFTER_DONE) {
			data_collection.Reset();
		}
		scan_status = RadixHTScanStatus::DONE;
		auto guard = sink.Lock();
		if (++gstate.task_done == sink.partitions.size()) {
			gstate.finished = true;
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

	if (sink.count_before_combining == 0) {
		if (grouping_set.empty()) {
			// Special case hack to sort out aggregating from empty intermediates for aggregations without groups
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
				auto aggr_state = make_unsafe_uniq_array_uninitialized<data_t>(aggr.function.state_size(aggr.function));
				aggr.function.initialize(aggr.function, aggr_state.get());

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
		}
		gstate.finished = true;
		return SourceResultType::FINISHED;
	}

	while (!gstate.finished && chunk.size() == 0) {
		if (lstate.TaskFinished()) {
			const auto res = gstate.AssignTask(sink, lstate, input.interrupt_state);
			if (res != SourceResultType::HAVE_MORE_OUTPUT) {
				D_ASSERT(res == SourceResultType::FINISHED || res == SourceResultType::BLOCKED);
				return res;
			}
		}
		lstate.ExecuteTask(sink, gstate, chunk);
	}

	if (chunk.size() != 0) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
		return SourceResultType::FINISHED;
	}
}

ProgressData RadixPartitionedHashTable::GetProgress(ClientContext &, GlobalSinkState &sink_p,
                                                    GlobalSourceState &gstate_p) const {
	auto &sink = sink_p.Cast<RadixHTGlobalSinkState>();
	auto &gstate = gstate_p.Cast<RadixHTGlobalSourceState>();

	// Get partition combine progress, weigh it 2x
	ProgressData progress;
	for (auto &partition : sink.partitions) {
		progress.done += 2.0 * partition->progress;
	}

	// Get scan progress, weigh it 1x
	progress.done += 1.0 * double(gstate.task_done);

	// Divide by 3x for the weights, and the number of partitions to get a value between 0 and 1 again
	progress.total += 3.0 * double(sink.partitions.size());

	return progress;
}

} // namespace duckdb
