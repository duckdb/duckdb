#include "duckdb/common/sorting/sorted_run_merger.hpp"

#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "pdqsort.h"
#include "vergesort.h"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sorted Run Merger Utility
//===--------------------------------------------------------------------===//
static idx_t SortedRunsTotalCount(const vector<unique_ptr<SortedRun>> &sorted_runs) {
	idx_t total_count = 0;
	for (const auto &sorted_run : sorted_runs) {
		D_ASSERT(sorted_run->finalized);
		total_count += sorted_run->Count();
	}
	return total_count;
}

struct SortedRunPartitionBoundary {
	idx_t begin;
	idx_t end;
};

struct SortedRunMergePartition {
public:
	SortedRunMergePartition(const SortedRunMerger &merger, const idx_t partition_idx)
	    : start_computed(partition_idx == 0) {
		// Initialize each partition with sensible defaults
		run_boundaries.resize(merger.sorted_runs.size());
		const auto maximum_end = (partition_idx + 1) * merger.partition_size;
		for (idx_t run_idx = 0; run_idx < merger.sorted_runs.size(); run_idx++) {
			const auto end = MinValue(merger.sorted_runs[run_idx]->Count(), maximum_end);
			run_boundaries[run_idx] = {0, end};
		}
	}

public:
	unique_lock<mutex> Lock() {
		return unique_lock<mutex>(lock);
	}

	unsafe_vector<SortedRunPartitionBoundary> &GetRunBoundaries(const unique_lock<mutex> &guard) {
		VerifyLock(guard);
		return run_boundaries;
	}

	bool GetStartComputed(const unique_lock<mutex> &guard) const {
		VerifyLock(guard);
		return start_computed;
	}

	void SetStartComputed(const unique_lock<mutex> &guard) {
		VerifyLock(guard);
		start_computed = true;
	}

private:
	void VerifyLock(const unique_lock<mutex> &guard) const {
#ifdef DEBUG
		D_ASSERT(guard.mutex() && RefersToSameObject(*guard.mutex(), lock));
#endif
	}

private:
	mutex lock;
	unsafe_vector<SortedRunPartitionBoundary> run_boundaries;
	bool start_computed;
};

enum class SortedRunMergerTask : uint8_t {
	//! Compute boundaries of the assigned partition
	COMPUTE_BOUNDARIES,
	//! Acquire boundaries of the previous partition
	ACQUIRE_BOUNDARIES,
	//! Merge the assigned partition
	MERGE_PARTITION,
	//! Scan the merged partition
	SCAN_PARTITION,
	//! No task
	FINISHED,
};

//===--------------------------------------------------------------------===//
// Local State Header
//===--------------------------------------------------------------------===//
class SortedRunMergerGlobalState;

class SortedRunMergerLocalState : public LocalSourceState {
public:
	explicit SortedRunMergerLocalState(SortedRunMergerGlobalState &gstate_p);

public:
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished() const;
	//! Do the work this thread has been assigned
	void ExecuteTask(SortedRunMergerGlobalState &gstate, DataChunk &chunk);

private:
	//! Computes upper partition boundaries using K-way Merge Path
	void ComputePartitionBoundaries(SortedRunMergerGlobalState &gstate, const optional_idx &p_idx);
	template <class STATE>
	void ComputePartitionBoundariesSwitch(SortedRunMergerGlobalState &gstate, const optional_idx &p_idx,
	                                      unsafe_vector<STATE> &states);
	template <class STATE, SortKeyType SORT_KEY_TYPE>
	void TemplatedComputePartitionBoundaries(SortedRunMergerGlobalState &gstate, const optional_idx &p_idx,
	                                         unsafe_vector<STATE> &states);

	//! Acquires lower partition boundaries from the global state
	void AcquirePartitionBoundaries(SortedRunMergerGlobalState &gstate);

	//! Merge the partition to obtain the next chunk
	void MergePartition(SortedRunMergerGlobalState &gstate);
	template <class STATE>
	void MergePartitionSwitch(SortedRunMergerGlobalState &gstate, unsafe_vector<STATE> &states);
	template <class STATE, SortKeyType SORT_KEY_TYPE>
	void TemplatedMergePartition(SortedRunMergerGlobalState &gstate, unsafe_vector<STATE> &states);

	//! Scan from the merged partition
	void ScanPartition(SortedRunMergerGlobalState &gstate, DataChunk &chunk);
	template <SortKeyType SORT_KEY_TYPE>
	void TemplatedScanPartition(SortedRunMergerGlobalState &gstate, DataChunk &chunk);

public:
	//! Types for templating
	const BlockIteratorStateType iterator_state_type;
	const SortKeyType sort_key_type;

	SortedRunMergerTask task;
	optional_idx partition_idx;

private:
	//! Computed run boundaries
	unsafe_vector<SortedRunPartitionBoundary> run_boundaries;

	//! States for the iterator types
	unsafe_vector<BlockIteratorState<BlockIteratorStateType::IN_MEMORY>> in_memory_states;
	unsafe_vector<BlockIteratorState<BlockIteratorStateType::EXTERNAL>> external_states;

	//! Allocation for the partition
	AllocatedData merged_partition;

	//! Variables for scanning
	idx_t merged_partition_count;
	idx_t merged_partition_index;
	TupleDataScanState payload_state;
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
class SortedRunMergerGlobalState : public GlobalSourceState {
public:
	explicit SortedRunMergerGlobalState(ClientContext &context_p, const SortedRunMerger &merger_p)
	    : context(context_p), merger(merger_p), num_runs(merger.sorted_runs.size()),
	      num_partitions((merger.total_count + (merger.partition_size - 1)) / merger.partition_size),
	      iterator_state_type(GetBlockIteratorStateType(merger.external)),
	      sort_key_type(merger.key_layout->GetSortKeyType()), next_partition_idx(0), total_scanned(0) {
		// Initialize partitions
		partitions.resize(num_partitions);
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			partitions[partition_idx] = make_uniq<SortedRunMergePartition>(merger, partition_idx);
		}
	}

public:
	bool AssignTask(SortedRunMergerLocalState &lstate) {
		D_ASSERT(!lstate.partition_idx.IsValid());
		D_ASSERT(lstate.task == SortedRunMergerTask::FINISHED);
		auto guard = Lock();
		if (next_partition_idx == num_partitions) {
			return false; // Nothing left to do
		}
		lstate.partition_idx = next_partition_idx++;
		lstate.task = SortedRunMergerTask::COMPUTE_BOUNDARIES;
		return true;
	}

	idx_t MaxThreads() override {
		return MaxValue<idx_t>(num_partitions, 1);
	}

public:
	ClientContext &context;

	const SortedRunMerger &merger;
	const idx_t num_runs;
	const idx_t num_partitions;

	const BlockIteratorStateType iterator_state_type;
	const SortKeyType sort_key_type;

	idx_t next_partition_idx;
	vector<unique_ptr<SortedRunMergePartition>> partitions;
	atomic<idx_t> total_scanned;
};

//===--------------------------------------------------------------------===//
// Local State Implementation
//===--------------------------------------------------------------------===//
SortedRunMergerLocalState::SortedRunMergerLocalState(SortedRunMergerGlobalState &gstate)
    : iterator_state_type(gstate.iterator_state_type), sort_key_type(gstate.sort_key_type),
      task(SortedRunMergerTask::FINISHED), run_boundaries(gstate.num_runs),
      merged_partition_count(DConstants::INVALID_INDEX), merged_partition_index(DConstants::INVALID_INDEX) {
	for (const auto &run : gstate.merger.sorted_runs) {
		auto &key_data = *run->key_data;
		switch (iterator_state_type) {
		case BlockIteratorStateType::IN_MEMORY:
			in_memory_states.push_back(BlockIteratorState<BlockIteratorStateType::IN_MEMORY>(key_data));
			break;
		case BlockIteratorStateType::EXTERNAL:
			external_states.push_back(
			    BlockIteratorState<BlockIteratorStateType::EXTERNAL>(key_data, run->payload_data.get()));
			break;
		default:
			throw NotImplementedException("SortedRunMergerLocalState::SortedRunMergerLocalState for %s",
			                              EnumUtil::ToString(iterator_state_type));
		}
	}
}

bool SortedRunMergerLocalState::TaskFinished() const {
	switch (task) {
	case SortedRunMergerTask::COMPUTE_BOUNDARIES:
	case SortedRunMergerTask::ACQUIRE_BOUNDARIES:
	case SortedRunMergerTask::MERGE_PARTITION:
	case SortedRunMergerTask::SCAN_PARTITION:
		D_ASSERT(partition_idx.IsValid());
		return false;
	case SortedRunMergerTask::FINISHED:
		D_ASSERT(!partition_idx.IsValid());
		return true;
	default:
		throw NotImplementedException("SortedRunMergerLocalState::TaskFinished for task");
	}
}

void SortedRunMergerLocalState::ExecuteTask(SortedRunMergerGlobalState &gstate, DataChunk &chunk) {
	D_ASSERT(task != SortedRunMergerTask::FINISHED);
	switch (task) {
	case SortedRunMergerTask::COMPUTE_BOUNDARIES:
		ComputePartitionBoundaries(gstate, partition_idx);
		task = SortedRunMergerTask::ACQUIRE_BOUNDARIES;
		break;
	case SortedRunMergerTask::ACQUIRE_BOUNDARIES:
		AcquirePartitionBoundaries(gstate);
		task = SortedRunMergerTask::MERGE_PARTITION;
		break;
	case SortedRunMergerTask::MERGE_PARTITION:
		MergePartition(gstate);
		task = SortedRunMergerTask::SCAN_PARTITION;
		break;
	case SortedRunMergerTask::SCAN_PARTITION:
		ScanPartition(gstate, chunk);
		if (chunk.size() == 0) {
			task = SortedRunMergerTask::FINISHED;
			partition_idx = optional_idx::Invalid();
		}
		break;
	default:
		throw NotImplementedException("SortedRunMergerLocalState::ExecuteTask for task");
	}
}

void SortedRunMergerLocalState::ComputePartitionBoundaries(SortedRunMergerGlobalState &gstate,
                                                           const optional_idx &p_idx) {
	D_ASSERT(p_idx.IsValid());
	D_ASSERT(task == SortedRunMergerTask::COMPUTE_BOUNDARIES);

	// Copy over the run boundaries from the assigned partition (under lock)
	auto &current_partition = *gstate.partitions[partition_idx.GetIndex()];
	auto guard = current_partition.Lock();
	run_boundaries = current_partition.GetRunBoundaries(guard);
	guard.unlock();

	// Compute the end partition boundaries (lock-free)
	switch (iterator_state_type) {
	case BlockIteratorStateType::IN_MEMORY:
		ComputePartitionBoundariesSwitch<BlockIteratorState<BlockIteratorStateType::IN_MEMORY>>(gstate, p_idx,
		                                                                                        in_memory_states);
		break;
	case BlockIteratorStateType::EXTERNAL:
		ComputePartitionBoundariesSwitch<BlockIteratorState<BlockIteratorStateType::EXTERNAL>>(gstate, p_idx,
		                                                                                       external_states);
		break;
	default:
		throw NotImplementedException(
		    "SortedRunMergerLocalState::ComputePartitionBoundaries for SortedRunIteratorType");
	}

	// The computed boundaries of the current partition may be the start boundaries of the next partition
	// Another thread depends on this, set them first
	if (p_idx.GetIndex() != gstate.num_partitions - 1) {
		auto &next_partition = *gstate.partitions[p_idx.GetIndex() + 1];
		guard = next_partition.Lock();
		auto &next_partition_run_boundaries = next_partition.GetRunBoundaries(guard);
		for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
			const auto &computed_boundary = run_boundaries[run_idx];
			D_ASSERT(computed_boundary.begin == computed_boundary.end);
			next_partition_run_boundaries[run_idx].begin = computed_boundary.begin;
		}
		next_partition.SetStartComputed(guard);
		guard.unlock();
	}

	// Set the computed end partition boundaries of the current partition
	guard = current_partition.Lock();
	auto &current_partition_run_boundaries = current_partition.GetRunBoundaries(guard);
	for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
		const auto &computed_boundary = run_boundaries[run_idx];
		D_ASSERT(computed_boundary.begin == computed_boundary.end);
		current_partition_run_boundaries[run_idx].end = computed_boundary.end;
	}
}

template <class STATE>
void SortedRunMergerLocalState::ComputePartitionBoundariesSwitch(SortedRunMergerGlobalState &gstate,
                                                                 const optional_idx &p_idx,
                                                                 unsafe_vector<STATE> &states) {
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_8>(gstate, p_idx, states);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_16>(gstate, p_idx, states);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_24>(gstate, p_idx, states);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_32>(gstate, p_idx, states);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_VARIABLE_32>(gstate, p_idx, states);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_FIXED_16>(gstate, p_idx, states);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_FIXED_24>(gstate, p_idx, states);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_FIXED_32>(gstate, p_idx, states);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_VARIABLE_32>(gstate, p_idx, states);
	default:
		throw NotImplementedException("SortedRunMergerLocalState::ComputePartitionBoundariesSwitch for %s",
		                              EnumUtil::ToString(sort_key_type));
	}
}

static idx_t ComputeBoundaryDelta(const idx_t &total_remaining, const idx_t &k,
                                  const SortedRunPartitionBoundary &run_boundary) {
	D_ASSERT(run_boundary.begin < run_boundary.end);
	const auto run_remaining = run_boundary.end - run_boundary.begin;
	return MinValue(AlignValue(total_remaining, k) / k, run_remaining);
}

template <class STATE, SortKeyType SORT_KEY_TYPE>
void SortedRunMergerLocalState::TemplatedComputePartitionBoundaries(SortedRunMergerGlobalState &gstate,
                                                                    const optional_idx &p_idx,
                                                                    unsafe_vector<STATE> &states) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCK_ITERATOR = block_iterator_t<STATE, SORT_KEY>;

	D_ASSERT(run_boundaries.size() == gstate.num_runs);

	// Check if last partition: boundary is always end of each sorted run
	if (p_idx == gstate.num_partitions - 1) {
		for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
			run_boundaries[run_idx].begin = gstate.merger.sorted_runs[run_idx]->Count();
		}
		return;
	}

	// Initialize "total_remaining", i.e., how much we still need to update the boundaries until we're done
	idx_t total_remaining = (p_idx.GetIndex() + 1) * gstate.merger.partition_size;

	// Initialize iterators, and track of which runs are actively being used in the computation, i.e., not yet fixed
	unsafe_vector<BLOCK_ITERATOR> run_iterators;
	run_iterators.reserve(gstate.num_runs);
	unsafe_vector<idx_t> active_run_idxs(gstate.num_runs);
	for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
		auto &state = states[run_idx];
		state.Unpin();

		run_iterators.emplace_back(state);
		active_run_idxs[run_idx] = run_idx;

		// Reduce total remaining by what we already have (from previous partition)
		D_ASSERT(run_boundaries[run_idx].begin <= total_remaining);
		total_remaining -= run_boundaries[run_idx].begin;
	}

	// Deltas per run, used for convenience below
	unsafe_vector<idx_t> run_deltas(gstate.num_runs);

	D_ASSERT(total_remaining <
	         gstate.merger.total_count); // This is not the last partition, so should be less than total
	while (total_remaining != 0) {
		D_ASSERT(!active_run_idxs.empty());
		const idx_t k = active_run_idxs.size();

		// Compute step size for each run: ceil(total_remaining / k), or less if run has less remaining
		auto min_idx = active_run_idxs[0];
		auto min_delta = ComputeBoundaryDelta(total_remaining, k, run_boundaries[min_idx]);
		reference<SORT_KEY> min_value = run_iterators[min_idx][run_boundaries[min_idx].begin + min_delta - 1];
		for (idx_t i = 1; i < active_run_idxs.size(); i++) {
			const auto &active_run_idx = active_run_idxs[i];
			const auto &run_boundary = run_boundaries[active_run_idx];
			const auto run_delta = ComputeBoundaryDelta(total_remaining, k, run_boundary);
			auto &run_value = run_iterators[active_run_idx][run_boundary.begin + run_delta - 1];
			if (run_value < min_value.get()) {
				min_idx = active_run_idx;
				min_delta = run_delta;
				min_value = run_value;
			}
		}

		// Increment boundary begin value of the min run by the delta
		auto &min_run_boundary = run_boundaries[min_idx];
		min_run_boundary.begin += min_delta;

		// Erase from active if begin is equal to end
		if (min_run_boundary.begin == min_run_boundary.end) {
			active_run_idxs.erase(std::find(active_run_idxs.begin(), active_run_idxs.end(), min_idx));
		}

		// Update total remaining accordingly
		D_ASSERT(min_delta <= total_remaining);
		total_remaining -= min_delta;
	}

	// End of boundary is meaningless now, just set equal to begin so there's no confusion
	for (auto &run_boundary : run_boundaries) {
		run_boundary.end = run_boundary.begin;
	}
}

void SortedRunMergerLocalState::AcquirePartitionBoundaries(SortedRunMergerGlobalState &gstate) {
	D_ASSERT(partition_idx.IsValid());
	D_ASSERT(task == SortedRunMergerTask::ACQUIRE_BOUNDARIES);
	auto &current_partition = *gstate.partitions[partition_idx.GetIndex()];
	auto guard = current_partition.Lock();
	if (current_partition.GetStartComputed(guard)) {
		// Start has been computed, boundaries are ready to use. Copy to local
		run_boundaries = current_partition.GetRunBoundaries(guard);
		return;
	}
	guard.unlock();

	// Start has not yet been computed by another thread, just let this thread do it
	task = SortedRunMergerTask::COMPUTE_BOUNDARIES;
	ComputePartitionBoundaries(gstate, partition_idx.GetIndex() - 1);
	task = SortedRunMergerTask::ACQUIRE_BOUNDARIES;

	// Copy to local
	guard.lock();
	D_ASSERT(current_partition.GetStartComputed(guard));
	run_boundaries = current_partition.GetRunBoundaries(guard);
}

void SortedRunMergerLocalState::MergePartition(SortedRunMergerGlobalState &gstate) {
	switch (iterator_state_type) {
	case BlockIteratorStateType::IN_MEMORY:
		MergePartitionSwitch<BlockIteratorState<BlockIteratorStateType::IN_MEMORY>>(gstate, in_memory_states);
		break;
	case BlockIteratorStateType::EXTERNAL:
		MergePartitionSwitch<BlockIteratorState<BlockIteratorStateType::EXTERNAL>>(gstate, external_states);
		break;
	default:
		throw NotImplementedException("SortedRunMergerLocalState::MergePartition for %s",
		                              EnumUtil::ToString(iterator_state_type));
	}
}

template <class STATE>
void SortedRunMergerLocalState::MergePartitionSwitch(SortedRunMergerGlobalState &gstate, unsafe_vector<STATE> &states) {
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedMergePartition<STATE, SortKeyType::NO_PAYLOAD_FIXED_8>(gstate, states);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedMergePartition<STATE, SortKeyType::NO_PAYLOAD_FIXED_16>(gstate, states);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedMergePartition<STATE, SortKeyType::NO_PAYLOAD_FIXED_24>(gstate, states);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedMergePartition<STATE, SortKeyType::NO_PAYLOAD_FIXED_32>(gstate, states);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedMergePartition<STATE, SortKeyType::NO_PAYLOAD_VARIABLE_32>(gstate, states);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedMergePartition<STATE, SortKeyType::PAYLOAD_FIXED_16>(gstate, states);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedMergePartition<STATE, SortKeyType::PAYLOAD_FIXED_24>(gstate, states);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedMergePartition<STATE, SortKeyType::PAYLOAD_FIXED_32>(gstate, states);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedMergePartition<STATE, SortKeyType::PAYLOAD_VARIABLE_32>(gstate, states);
	default:
		throw NotImplementedException("SortedRunMergerLocalState::MergePartitionSwitch for %s",
		                              EnumUtil::ToString(sort_key_type));
	}
}

template <class STATE, SortKeyType SORT_KEY_TYPE>
void SortedRunMergerLocalState::TemplatedMergePartition(SortedRunMergerGlobalState &gstate,
                                                        unsafe_vector<STATE> &states) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCK_ITERATOR = block_iterator_t<STATE, SORT_KEY>;

	if (!merged_partition.IsSet()) {
		merged_partition =
		    BufferAllocator::Get(gstate.context).Allocate(gstate.merger.partition_size * sizeof(SORT_KEY));
	}
	auto merged_partition_keys = reinterpret_cast<SORT_KEY *>(merged_partition.get());
	merged_partition_count = 0;
	merged_partition_index = 0;

	idx_t active_runs = 0;
	for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
		// Unpin previously held pins
		auto &state = states[run_idx];
		state.Unpin();

		// Keep track of how many runs are actively being merged
		const auto &run_boundary = run_boundaries[run_idx];
		active_runs += run_boundary.end != run_boundary.begin;

		for (auto it = BLOCK_ITERATOR(state, run_boundary.begin); it != BLOCK_ITERATOR(state, run_boundary.end); ++it) {
			merged_partition_keys[merged_partition_count++] = *it;
		}
	}

	if (active_runs == 1) {
		return; // Only one active run, no need to sort
	}

	// Seems counter-intuitive to re-sort instead of merging, but modern sorting algorithms detect and merge
	static const auto fallback = [](SORT_KEY *begin, SORT_KEY *end) {
		duckdb_pdqsort::pdqsort_branchless(begin, end);
	};
	duckdb_vergesort::vergesort(merged_partition_keys, merged_partition_keys + merged_partition_count,
	                            std::less<SORT_KEY>(), fallback);
}

void SortedRunMergerLocalState::ScanPartition(SortedRunMergerGlobalState &gstate, DataChunk &chunk) {
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedScanPartition<SortKeyType::NO_PAYLOAD_FIXED_8>(gstate, chunk);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedScanPartition<SortKeyType::NO_PAYLOAD_FIXED_16>(gstate, chunk);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedScanPartition<SortKeyType::NO_PAYLOAD_FIXED_24>(gstate, chunk);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedScanPartition<SortKeyType::NO_PAYLOAD_FIXED_32>(gstate, chunk);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedScanPartition<SortKeyType::NO_PAYLOAD_VARIABLE_32>(gstate, chunk);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedScanPartition<SortKeyType::PAYLOAD_FIXED_16>(gstate, chunk);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedScanPartition<SortKeyType::PAYLOAD_FIXED_24>(gstate, chunk);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedScanPartition<SortKeyType::PAYLOAD_FIXED_32>(gstate, chunk);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedScanPartition<SortKeyType::PAYLOAD_VARIABLE_32>(gstate, chunk);
	default:
		throw NotImplementedException("SortedRunMergerLocalState::ScanPartition for %s",
		                              EnumUtil::ToString(sort_key_type));
	}
}

template <SortKeyType SORT_KEY_TYPE>
void SortedRunMergerLocalState::TemplatedScanPartition(SortedRunMergerGlobalState &gstate, DataChunk &chunk) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	const auto count = MinValue<idx_t>(merged_partition_count - merged_partition_index, STANDARD_VECTOR_SIZE);

	const auto &output_projection_columns = gstate.merger.output_projection_columns;
	idx_t opc_idx = 0;

	// Decode from key
	for (; opc_idx < output_projection_columns.size(); opc_idx++) {
		const auto &opc = output_projection_columns[opc_idx];
		if (opc.is_payload) {
			break;
		}
		throw NotImplementedException("Sort");
	}

	// If there are no payload columns, we're done here
	if (opc_idx == output_projection_columns.size()) {
		return;
	}

	// Gather row pointers from keys
	const auto merged_partition_keys = reinterpret_cast<SORT_KEY *>(merged_partition.get()) + merged_partition_index;
	const auto payload_ptrs = FlatVector::GetData<data_ptr_t>(payload_state.chunk_state.row_locations);
	for (idx_t i = 0; i < count; i++) {
		payload_ptrs[i] = merged_partition_keys[i].GetPayload();
	}

	// Init scan state
	auto &payload_data = *gstate.merger.sorted_runs.back()->payload_data;
	if (payload_state.pin_state.properties == TupleDataPinProperties::INVALID) {
		payload_data.InitializeScan(payload_state, TupleDataPinProperties::ALREADY_PINNED);
	}
	TupleDataCollection::ResetCachedCastVectors(payload_state.chunk_state, payload_state.chunk_state.column_ids);

	// Now gather from payload
	for (; opc_idx < output_projection_columns.size(); opc_idx++) {
		const auto &opc = output_projection_columns[opc_idx];
		D_ASSERT(opc.is_payload);
		payload_data.Gather(payload_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(), count,
		                    opc.layout_col_idx, chunk.data[opc.output_col_idx],
		                    *FlatVector::IncrementalSelectionVector(),
		                    payload_state.chunk_state.cached_cast_vectors[opc.layout_col_idx]);
	}

	merged_partition_index += count;
	chunk.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Sorted Run Merger
//===--------------------------------------------------------------------===//
SortedRunMerger::SortedRunMerger(shared_ptr<TupleDataLayout> key_layout_p,
                                 vector<unique_ptr<SortedRun>> &&sorted_runs_p,
                                 const vector<SortProjectionColumn> &output_projection_columns_p,
                                 idx_t partition_size_p, bool external_p)
    : key_layout(std::move(key_layout_p)), sorted_runs(std::move(sorted_runs_p)),
      output_projection_columns(output_projection_columns_p), total_count(SortedRunsTotalCount(sorted_runs)),
      partition_size(partition_size_p), external(external_p) {
}

unique_ptr<LocalSourceState> SortedRunMerger::GetLocalSourceState(ExecutionContext &,
                                                                  GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<SortedRunMergerGlobalState>();
	return make_uniq<SortedRunMergerLocalState>(gstate);
}

unique_ptr<GlobalSourceState> SortedRunMerger::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<SortedRunMergerGlobalState>(context, *this);
}

SourceResultType SortedRunMerger::GetData(ExecutionContext &, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<SortedRunMergerGlobalState>();
	auto &lstate = input.local_state.Cast<SortedRunMergerLocalState>();

	while (chunk.size() == 0) {
		if (!lstate.TaskFinished() || gstate.AssignTask(lstate)) {
			lstate.ExecuteTask(gstate, chunk);
		} else {
			break;
		}
	}

	gstate.total_scanned += chunk.size();
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

OperatorPartitionData SortedRunMerger::GetPartitionData(ExecutionContext &, DataChunk &, GlobalSourceState &,
                                                        LocalSourceState &lstate_p,
                                                        const OperatorPartitionInfo &) const {
	auto &lstate = lstate_p.Cast<SortedRunMergerLocalState>();
	return OperatorPartitionData(lstate.partition_idx.GetIndex());
}

ProgressData SortedRunMerger::GetProgress(ClientContext &, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<SortedRunMergerGlobalState>();
	ProgressData res;
	res.done = static_cast<double>(gstate.total_scanned);
	res.total = static_cast<double>(total_count);
	res.invalid = false;
	return res;
}

} // namespace duckdb
