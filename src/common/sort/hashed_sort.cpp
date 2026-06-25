#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// HashedSortGroup
//===--------------------------------------------------------------------===//
class HashedSortGroup {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	HashedSortGroup(ClientContext &client, Sort &sort, idx_t group_idx);

	const idx_t group_idx;
	atomic<idx_t> count;

	//	Sink
	Sort &sort;
	unique_ptr<GlobalSinkState> sort_global;

	//	Source
	mutex scan_lock;
	TupleDataParallelScanState parallel_scan;
	atomic<idx_t> tasks_completed;
	unique_ptr<GlobalSourceState> sort_source;
	unique_ptr<ColumnDataCollection> direct_column_data;
};

HashedSortGroup::HashedSortGroup(ClientContext &client, Sort &sort, idx_t group_idx)
    : group_idx(group_idx), count(0), sort(sort), tasks_completed(0) {
	sort_global = sort.GetGlobalSinkState(client);
}

enum class HashedSortKeyState : uint8_t { EMPTY, SINGLE_KEY, MULTIPLE_KEYS };

class HashedSortKeyTracker {
public:
	HashedSortKeyTracker(Allocator &allocator_p, const vector<LogicalType> &key_types_p)
	    : allocator(allocator_p), key_types(key_types_p), key_count(key_types.size()) {
		single_value_sel.Initialize();
		candidate_input_sel.Initialize();
		candidate_rep_sel.Initialize();
		mismatch_sel.Initialize();
	}

	void Reset(idx_t radix_bits_p) {
		radix_bits = radix_bits_p;
		const auto partition_count = RadixPartitioning::NumberOfPartitions(radix_bits);
		states.clear();
		states.resize(partition_count, HashedSortKeyState::EMPTY);
		hashes.clear();
		hashes.resize(partition_count);
		representatives.Destroy();
		representatives.Initialize(allocator, key_types, partition_count);
		representatives.SetChildCardinality(partition_count);
	}

	bool CanBypass(idx_t hash_bin) const {
		return hash_bin < states.size() && states[hash_bin] == HashedSortKeyState::SINGLE_KEY;
	}

	void Update(DataChunk &keys, Vector &hashes, PartitionedTupleDataAppendState &append_state, idx_t count) {
		if (states.empty() || !count || AllTouchedBinsMixed(append_state)) {
			return;
		}

		D_ASSERT(keys.ColumnCount() == key_count);
		D_ASSERT(count <= STANDARD_VECTOR_SIZE);

		UnifiedVectorFormat hash_data;
		hashes.ToUnifiedFormat(hash_data);
		const auto hash_values = UnifiedVectorFormat::GetData<hash_t>(hash_data);

		idx_t candidate_count;
		if (append_state.fixed_partition_entries.size()) {
			const auto use_partition_sel = append_state.fixed_partition_entries.size() > 1;
			candidate_count =
			    BuildCandidates<true>(keys, hash_data, hash_values, append_state, count, use_partition_sel);
		} else {
			const auto use_partition_sel = append_state.partition_entries.size() > 1;
			candidate_count =
			    BuildCandidates<false>(keys, hash_data, hash_values, append_state, count, use_partition_sel);
		}
		if (candidate_count) {
			CompareCandidates(keys, candidate_count);
		}
	}

	void Combine(const HashedSortKeyTracker &other) {
		if (other.states.empty()) {
			return;
		}
		if (states.empty()) {
			Reset(other.radix_bits);
		}
		D_ASSERT(radix_bits == other.radix_bits);
		D_ASSERT(states.size() == other.states.size());
		D_ASSERT(states.size() <= STANDARD_VECTOR_SIZE);

		idx_t candidate_count = 0;
		for (idx_t bin_idx = 0; bin_idx < states.size(); bin_idx++) {
			CombineBin(other, bin_idx, candidate_count);
		}
		if (candidate_count) {
			CompareTrackerCandidates(other, candidate_count);
		}
	}

private:
	bool AllTouchedBinsMixed(PartitionedTupleDataAppendState &append_state) const {
		bool found = false;
		if (append_state.fixed_partition_entries.size()) {
			for (auto it = append_state.fixed_partition_entries.begin();
			     it != append_state.fixed_partition_entries.end(); ++it) {
				found = true;
				if (!IsMixed(it.GetKey())) {
					return false;
				}
			}
		} else {
			for (auto &entry : append_state.partition_entries) {
				found = true;
				if (!IsMixed(entry.first)) {
					return false;
				}
			}
		}
		return found;
	}

	bool IsMixed(idx_t bin_idx) const {
		D_ASSERT(bin_idx < states.size());
		return states[bin_idx] == HashedSortKeyState::MULTIPLE_KEYS;
	}

	void StoreRepresentative(DataChunk &keys, idx_t row_idx, hash_t hash, idx_t bin_idx) {
		states[bin_idx] = HashedSortKeyState::SINGLE_KEY;
		this->hashes[bin_idx] = hash;
		single_value_sel.set_index(0, row_idx);
		for (idx_t col_idx = 0; col_idx < key_count; col_idx++) {
			representatives.data[col_idx].Copy(keys.data[col_idx], single_value_sel, 1, 0, bin_idx, 1);
		}
	}

	void StoreRepresentative(const HashedSortKeyTracker &source, idx_t source_bin, idx_t target_bin) {
		states[target_bin] = HashedSortKeyState::SINGLE_KEY;
		hashes[target_bin] = source.hashes[source_bin];
		single_value_sel.set_index(0, source_bin);
		for (idx_t col_idx = 0; col_idx < key_count; col_idx++) {
			representatives.data[col_idx].Copy(source.representatives.data[col_idx], single_value_sel, 1, 0, target_bin,
			                                   1);
		}
	}

	void MarkMixed(idx_t bin_idx) {
		states[bin_idx] = HashedSortKeyState::MULTIPLE_KEYS;
	}

	void UpdateRow(DataChunk &keys, idx_t row_idx, hash_t hash, idx_t bin_idx, idx_t &candidate_count) {
		D_ASSERT(bin_idx < states.size());
		switch (states[bin_idx]) {
		case HashedSortKeyState::EMPTY:
			StoreRepresentative(keys, row_idx, hash, bin_idx);
			break;
		case HashedSortKeyState::SINGLE_KEY:
			if (this->hashes[bin_idx] != hash) {
				MarkMixed(bin_idx);
				break;
			}
			candidate_input_sel.set_index(candidate_count, row_idx);
			candidate_rep_sel.set_index(candidate_count, bin_idx);
			candidate_count++;
			break;
		case HashedSortKeyState::MULTIPLE_KEYS:
			break;
		}
	}

	template <bool FIXED>
	idx_t BuildCandidates(DataChunk &keys, UnifiedVectorFormat &hash_data, const hash_t *hash_values,
	                      PartitionedTupleDataAppendState &append_state, idx_t count, bool use_partition_sel) {
		using GETTER = TemplatedMapGetter<list_entry_t, FIXED>;
		auto &partition_entries = append_state.GetMap<FIXED>();
		idx_t candidate_count = 0;
		for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
			const auto bin_idx = GETTER::GetKey(it);
			const auto &entry = GETTER::GetValue(it);
			for (idx_t entry_idx = 0; entry_idx < entry.length; entry_idx++) {
				const auto row_idx =
				    use_partition_sel ? append_state.partition_sel.get_index(entry.offset + entry_idx) : entry_idx;
				D_ASSERT(row_idx < count);
				const auto hash_idx = hash_data.sel->get_index(row_idx);
				UpdateRow(keys, row_idx, hash_values[hash_idx], bin_idx, candidate_count);
			}
		}
		return candidate_count;
	}

	idx_t CompactCandidates(idx_t candidate_count) {
		idx_t new_count = 0;
		for (idx_t candidate_idx = 0; candidate_idx < candidate_count; candidate_idx++) {
			const auto bin_idx = candidate_rep_sel.get_index_unsafe(candidate_idx);
			if (states[bin_idx] != HashedSortKeyState::SINGLE_KEY) {
				continue;
			}
			candidate_input_sel.set_index(new_count, candidate_input_sel.get_index_unsafe(candidate_idx));
			candidate_rep_sel.set_index(new_count, bin_idx);
			new_count++;
		}
		return new_count;
	}

	void CompareCandidates(DataChunk &keys, idx_t candidate_count) {
		D_ASSERT(candidate_count <= STANDARD_VECTOR_SIZE);
		for (idx_t col_idx = 0; col_idx < key_count && candidate_count; col_idx++) {
			Vector input_slice(keys.data[col_idx], candidate_input_sel, candidate_count);
			Vector representative_slice(representatives.data[col_idx], candidate_rep_sel, candidate_count);
			const auto mismatch_count = VectorOperations::DistinctFrom(input_slice, representative_slice, nullptr,
			                                                           candidate_count, &mismatch_sel, nullptr);
			for (idx_t mismatch_idx = 0; mismatch_idx < mismatch_count; mismatch_idx++) {
				const auto candidate_idx = mismatch_sel.get_index_unsafe(mismatch_idx);
				MarkMixed(candidate_rep_sel.get_index_unsafe(candidate_idx));
			}
			if (mismatch_count && col_idx + 1 < key_count) {
				candidate_count = CompactCandidates(candidate_count);
			}
		}
	}

	void CombineBin(const HashedSortKeyTracker &source, idx_t bin_idx, idx_t &candidate_count) {
		if (source.states[bin_idx] == HashedSortKeyState::EMPTY ||
		    states[bin_idx] == HashedSortKeyState::MULTIPLE_KEYS) {
			return;
		}
		if (source.states[bin_idx] == HashedSortKeyState::MULTIPLE_KEYS) {
			MarkMixed(bin_idx);
			return;
		}
		D_ASSERT(source.states[bin_idx] == HashedSortKeyState::SINGLE_KEY);
		if (states[bin_idx] == HashedSortKeyState::EMPTY) {
			StoreRepresentative(source, bin_idx, bin_idx);
			return;
		}
		D_ASSERT(states[bin_idx] == HashedSortKeyState::SINGLE_KEY);
		if (hashes[bin_idx] != source.hashes[bin_idx]) {
			MarkMixed(bin_idx);
			return;
		}
		candidate_input_sel.set_index(candidate_count, bin_idx);
		candidate_rep_sel.set_index(candidate_count, bin_idx);
		candidate_count++;
	}

	idx_t CompactTrackerCandidates(idx_t candidate_count) {
		idx_t new_count = 0;
		for (idx_t candidate_idx = 0; candidate_idx < candidate_count; candidate_idx++) {
			const auto bin_idx = candidate_input_sel.get_index_unsafe(candidate_idx);
			if (states[bin_idx] != HashedSortKeyState::SINGLE_KEY) {
				continue;
			}
			candidate_input_sel.set_index(new_count, bin_idx);
			candidate_rep_sel.set_index(new_count, candidate_rep_sel.get_index_unsafe(candidate_idx));
			new_count++;
		}
		return new_count;
	}

	void CompareTrackerCandidates(const HashedSortKeyTracker &source, idx_t candidate_count) {
		D_ASSERT(candidate_count <= STANDARD_VECTOR_SIZE);
		for (idx_t col_idx = 0; col_idx < key_count && candidate_count; col_idx++) {
			Vector target_slice(representatives.data[col_idx], candidate_input_sel, candidate_count);
			Vector source_slice(source.representatives.data[col_idx], candidate_rep_sel, candidate_count);
			const auto mismatch_count = VectorOperations::DistinctFrom(target_slice, source_slice, nullptr,
			                                                           candidate_count, &mismatch_sel, nullptr);
			for (idx_t mismatch_idx = 0; mismatch_idx < mismatch_count; mismatch_idx++) {
				const auto candidate_idx = mismatch_sel.get_index_unsafe(mismatch_idx);
				MarkMixed(candidate_input_sel.get_index_unsafe(candidate_idx));
			}
			if (mismatch_count && col_idx + 1 < key_count) {
				candidate_count = CompactTrackerCandidates(candidate_count);
			}
		}
	}

private:
	Allocator &allocator;
	vector<LogicalType> key_types;
	idx_t key_count;
	idx_t radix_bits = 0;
	vector<HashedSortKeyState> states;
	vector<hash_t> hashes;
	DataChunk representatives;
	SelectionVector single_value_sel;
	SelectionVector candidate_input_sel;
	SelectionVector candidate_rep_sel;
	SelectionVector mismatch_sel;
};

static vector<LogicalType> GetHashedSortPartitionKeyTypes(const HashedSort &hashed_sort) {
	vector<LogicalType> key_types;
	key_types.reserve(hashed_sort.partition_key_count);
	for (idx_t key_idx = 0; key_idx < hashed_sort.partition_key_count; key_idx++) {
		key_types.push_back(hashed_sort.partitions[key_idx].expression->GetReturnType());
	}
	return key_types;
}

class HashedSortRepartitionObserver : public PartitionedTupleDataRepartitionObserver {
public:
	HashedSortRepartitionObserver(Allocator &allocator, HashedSortKeyTracker &tracker_p, const HashedSort &hashed_sort)
	    : tracker(tracker_p), partition_key_ids(hashed_sort.partition_key_ids),
	      hash_col_idx(hashed_sort.payload_types.size()), hash_vector(LogicalType::HASH) {
		keys.Initialize(allocator, GetHashedSortPartitionKeyTypes(hashed_sort));
	}

	void RepartitionChunk(TupleDataCollection &source_partition, TupleDataChunkState &source_chunk,
	                      PartitionedTupleDataAppendState &target_append, idx_t count) override {
		if (!count) {
			return;
		}

		if (key_gather_state.column_ids.empty()) {
			source_partition.InitializeChunkState(key_gather_state, partition_key_ids);
			source_partition.InitializeChunkState(hash_gather_state, {hash_col_idx});
		}

		keys.Reset();
		TupleDataCollection::ResetCachedCastVectors(key_gather_state, partition_key_ids);
		source_partition.Gather(source_chunk.row_locations, *FlatVector::IncrementalSelectionVector(), count,
		                        partition_key_ids, keys, *FlatVector::IncrementalSelectionVector(),
		                        key_gather_state.cached_cast_vectors);
		keys.SetChildCardinality(count);

		TupleDataCollection::ResetCachedCastVectors(hash_gather_state, hash_gather_state.column_ids);
		source_partition.Gather(source_chunk.row_locations, *FlatVector::IncrementalSelectionVector(), count,
		                        hash_col_idx, hash_vector, *FlatVector::IncrementalSelectionVector(),
		                        hash_gather_state.cached_cast_vectors[0].get());

		tracker.Update(keys, hash_vector, target_append, count);
	}

private:
	HashedSortKeyTracker &tracker;
	const vector<column_t> &partition_key_ids;
	column_t hash_col_idx;
	DataChunk keys;
	Vector hash_vector;
	TupleDataChunkState key_gather_state;
	TupleDataChunkState hash_gather_state;
};

//===--------------------------------------------------------------------===//
// HashedSortGlobalSinkState
//===--------------------------------------------------------------------===//
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
	void UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &partition_append,
	                          optional_ptr<HashedSortKeyTracker> local_tracker);
	void CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append,
	                           optional_ptr<HashedSortKeyTracker> local_tracker);
	ProgressData GetSinkProgress(ClientContext &context, const ProgressData source_progress) const;
	bool CanBypassSort(idx_t hash_bin) const;

	//! System and query state
	ClientContext &client;
	const HashedSort &hashed_sort;
	BufferManager &buffer_manager;
	Allocator &allocator;
	mutable mutex lock;

	// OVER(PARTITION BY...) (hash grouping)
	GroupingPartition grouping_data;
	//! Payload plus hash column
	shared_ptr<TupleDataLayout> grouping_types_ptr;
	unique_ptr<HashedSortKeyTracker> single_key_tracker;
	//! The number of radix bits if this partition is being synced with another
	idx_t fixed_bits;

	// OVER(...) (sorting)
	vector<HashGroupPtr> hash_groups;

	// Threading
	idx_t max_bits;
	atomic<idx_t> count;

private:
	void Rehash(idx_t cardinality);
	void SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append,
	                        optional_ptr<HashedSortKeyTracker> local_tracker);
};

HashedSortGlobalSinkState::HashedSortGlobalSinkState(ClientContext &client, const HashedSort &hashed_sort)
    : client(client), hashed_sort(hashed_sort), buffer_manager(BufferManager::GetBufferManager(client)),
      allocator(Allocator::Get(client)), fixed_bits(0), max_bits(1), count(0) {
	if (hashed_sort.can_bypass_single_key_sort) {
		single_key_tracker = make_uniq<HashedSortKeyTracker>(allocator, GetHashedSortPartitionKeyTypes(hashed_sort));
	}

	const auto memory_per_thread = PhysicalOperator::GetMaxThreadMemory(client);
	const auto thread_pages = PreviousPowerOfTwo(memory_per_thread / (4 * buffer_manager.GetBlockAllocSize()));
	while (max_bits < 8 && (thread_pages >> max_bits) > 1) {
		++max_bits;
	}

	grouping_types_ptr = make_shared_ptr<TupleDataLayout>();
	auto &payload_types = hashed_sort.payload_types;
	auto types = payload_types;
	types.push_back(LogicalType::HASH);
	grouping_types_ptr->Initialize(types, TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	Rehash(hashed_sort.estimated_cardinality);
}

unique_ptr<RadixPartitionedTupleData> HashedSortGlobalSinkState::CreatePartition(idx_t new_bits) const {
	auto &payload_types = hashed_sort.payload_types;
	const auto hash_col_idx = payload_types.size();
	return make_uniq<RadixPartitionedTupleData>(buffer_manager, grouping_types_ptr, MemoryTag::WINDOW, new_bits,
	                                            hash_col_idx);
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
		new_bits = MinValue(new_bits + 2, max_bits);
	}

	// Repartition the grouping data
	if (new_bits != bits) {
		grouping_data = CreatePartition(new_bits);
		if (single_key_tracker) {
			single_key_tracker->Reset(new_bits);
		}
	}
}

void HashedSortGlobalSinkState::SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append,
                                                   optional_ptr<HashedSortKeyTracker> local_tracker) {
	// We are done if the local_partition is right sized.
	const auto new_bits = grouping_data->GetRadixBits();
	if (local_partition->GetRadixBits() == new_bits) {
		return;
	}

	// If the local partition is now too small, flush it and reallocate
	auto new_partition = CreatePartition(new_bits);
	local_partition->FlushAppendState(*local_append);
	unique_ptr<HashedSortRepartitionObserver> observer;
	if (local_tracker) {
		local_tracker->Reset(new_bits);
		observer = make_uniq<HashedSortRepartitionObserver>(allocator, *local_tracker, hashed_sort);
	}
	local_partition->Repartition(client, *new_partition, observer.get());

	local_partition = std::move(new_partition);
	local_append = make_uniq<PartitionedTupleDataAppendState>();
	local_partition->InitializeAppendState(*local_append);
}

void HashedSortGlobalSinkState::UpdateLocalPartition(GroupingPartition &local_partition,
                                                     GroupingAppend &partition_append,
                                                     optional_ptr<HashedSortKeyTracker> local_tracker) {
	// First call: initialize the local partition
	if (!local_partition) {
		lock_guard<mutex> guard(lock);
		local_partition = CreatePartition(grouping_data->GetRadixBits());
		partition_append = make_uniq<PartitionedTupleDataAppendState>();
		local_partition->InitializeAppendState(*partition_append);
		if (local_tracker) {
			local_tracker->Reset(local_partition->GetRadixBits());
		}
		return;
	}

	// Check bits under lock, repartition outside lock, then check again
	while (true) {
		idx_t new_bits;
		{
			lock_guard<mutex> guard(lock);
			Rehash(count);
			new_bits = grouping_data->GetRadixBits();
		}

		if (local_partition->GetRadixBits() == new_bits) {
			return; // Already in sync
		}

		auto new_partition = CreatePartition(new_bits);
		local_partition->FlushAppendState(*partition_append);
		unique_ptr<HashedSortRepartitionObserver> observer;
		if (local_tracker) {
			local_tracker->Reset(new_bits);
			observer = make_uniq<HashedSortRepartitionObserver>(allocator, *local_tracker, hashed_sort);
		}
		local_partition->Repartition(client, *new_partition, observer.get());
		local_partition = std::move(new_partition);
		partition_append = make_uniq<PartitionedTupleDataAppendState>();
		local_partition->InitializeAppendState(*partition_append);
	}
}

void HashedSortGlobalSinkState::SyncPartitioning(const HashedSortGlobalSinkState &other) {
	fixed_bits = other.grouping_data ? other.grouping_data->GetRadixBits() : 0;

	const auto old_bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	if (fixed_bits != old_bits) {
		grouping_data = CreatePartition(fixed_bits);
		if (single_key_tracker) {
			single_key_tracker->Reset(fixed_bits);
		}
	}
}

void HashedSortGlobalSinkState::CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append,
                                                      optional_ptr<HashedSortKeyTracker> local_tracker) {
	if (!local_partition) {
		return;
	}
	local_partition->FlushAppendState(*local_append);

	// Make sure grouping_data doesn't change under us.
	// Combine has an internal mutex, so this is single-threaded anyway.
	lock_guard<mutex> guard(lock);
	SyncLocalPartition(local_partition, local_append, local_tracker);
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
			hash_group = make_uniq<HashedSortGroup>(client, *hashed_sort.sort, group_idx);
		}
	}

	//	Combine the thread data into the global data
	if (single_key_tracker && local_tracker) {
		single_key_tracker->Combine(*local_tracker);
	}
	grouping_data->Combine(*local_partition);
}

bool HashedSortGlobalSinkState::CanBypassSort(idx_t hash_bin) const {
	return single_key_tracker && single_key_tracker->CanBypass(hash_bin);
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
		partition.InitializeScan(parallel_scan, partition_ids);
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

	//! OVER(PARTITION BY...) (hash grouping)
	GroupingPartition local_grouping;
	GroupingAppend grouping_append;
	unique_ptr<HashedSortKeyTracker> single_key_tracker;

	//! (optional) HyperLogLog state
	optional_ptr<ParallelHyperLogLogLocalState> hll;
};

HashedSortLocalSinkState::HashedSortLocalSinkState(ExecutionContext &context, const HashedSort &hashed_sort)
    : hashed_sort(hashed_sort), allocator(Allocator::Get(context.client)), hash_exec(context.client),
      sort_exec(context.client) {
	vector<LogicalType> group_types;
	for (idx_t prt_idx = 0; prt_idx < hashed_sort.partitions.size(); prt_idx++) {
		auto &pexpr = *hashed_sort.partitions[prt_idx].expression.get();
		group_types.push_back(pexpr.GetReturnType());
		hash_exec.AddExpression(pexpr);
	}

	vector<LogicalType> sort_types;
	for (const auto &expr : hashed_sort.sort_exprs) {
		sort_types.emplace_back(expr->GetReturnType());
		sort_exec.AddExpression(*expr);
	}
	sort_chunk.Initialize(context.client, sort_types);

	auto hash_types = hashed_sort.payload_types;
	group_chunk.Initialize(allocator, group_types);
	hash_types.emplace_back(LogicalType::HASH);
	payload_chunk.Initialize(allocator, hash_types);

	if (hashed_sort.can_bypass_single_key_sort) {
		single_key_tracker = make_uniq<HashedSortKeyTracker>(allocator, group_types);
	}
}

void HashedSort::Synchronize(const GlobalSinkState &source, GlobalSinkState &target) const {
	auto &src = source.Cast<HashedSortGlobalSinkState>();
	auto &tgt = target.Cast<HashedSortGlobalSinkState>();
	tgt.SyncPartitioning(src);
}

void HashedSortLocalSinkState::Hash(DataChunk &input_chunk, Vector &hash_vector) {
	D_ASSERT(group_chunk.ColumnCount() > 0);

	// OVER(PARTITION BY...) (hash grouping)
	group_chunk.Reset();
	hash_exec.Execute(input_chunk, group_chunk);
	const idx_t count = group_chunk.size();
	VectorOperations::Hash(group_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < group_chunk.ColumnCount(); ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, group_chunk.data[prt_idx], count);
	}
}

SinkResultType HashedSort::Sink(ExecutionContext &context, DataChunk &input_chunk, OperatorSinkInput &sink) const {
	auto &gstate = sink.global_state.Cast<HashedSortGlobalSinkState>();
	auto &lstate = sink.local_state.Cast<HashedSortLocalSinkState>();
	gstate.count += input_chunk.size();

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
		ConstantVector::SetNull(vec, count_t(input_chunk.size()));
	}

	payload_chunk.SetChildCardinality(input_chunk.size());

	// OVER(PARTITION BY...)
	auto &hash_vector = payload_chunk.data.back();
	lstate.Hash(input_chunk, hash_vector);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}

	// Update HLL state with hashes if necessary
	if (lstate.hll) {
		lstate.hll->Update(hash_vector);
	}

	auto &local_grouping = lstate.local_grouping;
	auto &grouping_append = lstate.grouping_append;
	gstate.UpdateLocalPartition(local_grouping, grouping_append, lstate.single_key_tracker.get());
	local_grouping->Append(*grouping_append, payload_chunk);
	if (lstate.single_key_tracker) {
		lstate.single_key_tracker->Update(lstate.group_chunk, hash_vector, *grouping_append, input_chunk.size());
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType HashedSort::Combine(ExecutionContext &context, OperatorSinkCombineInput &combine) const {
	auto &gstate = combine.global_state.Cast<HashedSortGlobalSinkState>();
	auto &lstate = combine.local_state.Cast<HashedSortLocalSinkState>();

	// OVER(PARTITION BY...)
	auto &local_grouping = lstate.local_grouping;
	if (!local_grouping) {
		return SinkCombineResultType::FINISHED;
	}

	// Flush our data and lock the bit count
	auto &grouping_append = lstate.grouping_append;
	gstate.CombineLocalPartition(local_grouping, grouping_append, lstate.single_key_tracker.get());

	return SinkCombineResultType::FINISHED;
}

void HashedSort::SortColumnData(ExecutionContext &context, hash_t hash_bin, OperatorSinkFinalizeInput &finalize) const {
	auto &gstate = finalize.global_state.Cast<HashedSortGlobalSinkState>();

	//	Loop over the partitions and add them to each hash group's global sort state
	auto &partitions = gstate.grouping_data->GetPartitions();
	if (hash_bin >= partitions.size()) {
		return;
	}

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

	if (gstate.CanBypassSort(hash_bin)) {
		auto local_column_data = make_uniq<ColumnDataCollection>(context.client, payload_types,
		                                                         ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
		ColumnDataAppendState append_state;
		local_column_data->InitializeAppend(append_state);

		idx_t combined = 0;
		while (partition.Scan(hash_group.parallel_scan, local_scan, chunk)) {
			local_column_data->Append(append_state, chunk);
			combined += chunk.size();
		}

		lock_guard<mutex> direct_guard(hash_group.scan_lock);
		if (combined) {
			if (!hash_group.direct_column_data) {
				hash_group.direct_column_data = std::move(local_column_data);
			} else {
				hash_group.direct_column_data->Combine(*local_column_data);
			}
			hash_group.count += combined;
		}
		return;
	}

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
		hash_group.sort_source = sort->GetGlobalSourceState(context.client, *hash_group.sort_global);
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
    : SortStrategy(input_types), estimated_cardinality(estimated_cardinality) {
	GenerateOrderings(partitions, orders, partition_bys, order_bys, partition_stats);
	partition_key_count = partitions.size();
	can_bypass_single_key_sort = order_bys.empty();

	//	We have to compute ordering expressions ourselves and materialise them.
	//	To do this, we scan the orders and add generate extra payload columns that we can reference.
	for (auto &order : orders) {
		auto &expr = *order.expression;
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = expr.Cast<BoundReferenceExpression>();
			sort_ids.emplace_back(ref.Index());
			continue;
		}

		//	Real expression - replace with a ref and save the expression
		auto saved = std::move(order.expression);
		const auto type = saved->GetReturnType();
		const auto idx = payload_types.size();
		order.expression = make_uniq<BoundReferenceExpression>(type, idx);
		sort_ids.emplace_back(idx);
		payload_types.emplace_back(type);
		sort_exprs.emplace_back(std::move(saved));
	}
	for (idx_t key_idx = 0; key_idx < partition_key_count; key_idx++) {
		partition_key_ids.push_back(sort_ids[key_idx]);
	}

	// If a payload column is required, check whether there is one already
	if (require_payload) {
		//	Watch out for duplicate sort keys!
		unordered_set<column_t> sort_set(sort_ids.begin(), sort_ids.end());
		force_payload = (sort_set.size() >= payload_types.size());
		if (force_payload) {
			payload_types.emplace_back(LogicalType::BOOLEAN);
		}
	}

	// Remember the full set of materialised partition columns
	for (column_t i = 0; i < payload_types.size(); ++i) {
		partition_ids.emplace_back(i);
	}

	vector<idx_t> projection_map;
	sort = make_uniq<Sort>(client, orders, payload_types, projection_map);
}

unique_ptr<GlobalSinkState> HashedSort::GetGlobalSinkState(ClientContext &client) const {
	return make_uniq<HashedSortGlobalSinkState>(client, *this);
}

unique_ptr<LocalSinkState> HashedSort::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<HashedSortLocalSinkState>(context, *this);
}

void HashedSort::RegisterHyperLogLog(LocalSinkState &local_state, ParallelHyperLogLogLocalState &hll_state) const {
	auto &lstate = local_state.Cast<HashedSortLocalSinkState>();
	lstate.hll = hll_state;
}

unique_ptr<GlobalSourceState> HashedSort::GetGlobalSourceState(ClientContext &client, GlobalSinkState &sink) const {
	return make_uniq<HashedSortGlobalSourceState>(client, sink.Cast<HashedSortGlobalSinkState>());
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

	//	OVER(PARTITION BY...)
	if (gsink.grouping_data) {
		lock_guard<mutex> reset_guard(hash_group.scan_lock);
		auto &partitions = gsink.grouping_data->GetPartitions();
		if (hash_bin < partitions.size()) {
			//	Release the memory now that we have finished scanning it.
			partitions[hash_bin].reset();
		}
	}

	if (!build_runs && gsink.CanBypassSort(hash_bin)) {
		return SourceResultType::FINISHED;
	}

	auto &sort = hash_group.sort;
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

	if (gsink.CanBypassSort(hash_bin)) {
		auto result = std::move(hash_group.direct_column_data);
		if (result && result->Count() == hash_group.count) {
			return result;
		}
		return nullptr;
	}

	auto &sort = hash_group.sort;
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

	auto &sort = hash_group.sort;
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
