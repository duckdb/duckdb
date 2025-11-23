#include "duckdb/common/sorting/sorted_run_merger.hpp"

#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

#include "vergesort.h"
#include "pdqsort.h"

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
	    : begin_computed(partition_idx == 0), scanned(false) {
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

	bool GetBeginComputed() const {
		return begin_computed;
	}

	void SetBeginComputed() {
		begin_computed = true;
	}

private:
	void VerifyLock(const unique_lock<mutex> &guard) const {
#ifdef D_ASSERT_IS_ENABLED
		D_ASSERT(guard.mutex() && RefersToSameObject(*guard.mutex(), lock));
#endif
	}

private:
	mutex lock;
	unsafe_vector<SortedRunPartitionBoundary> run_boundaries;
	atomic<bool> begin_computed;

public:
	atomic<bool> scanned;
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
	SourceResultType ExecuteTask(SortedRunMergerGlobalState &gstate, optional_ptr<DataChunk> chunk);

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

	//! Materialize the merge
	void MaterializePartition(SortedRunMergerGlobalState &gstate);
	template <SortKeyType SORT_KEY_TYPE>
	unique_ptr<SortedRun> TemplatedMaterializePartition(SortedRunMergerGlobalState &gstate);

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

	//! Allocation for merging/scanning the partition
	AllocatedData merged_partition;

	//! Variables for scanning
	idx_t merged_partition_count;
	idx_t merged_partition_index;

	//! For scanning
	Vector sort_key_pointers;
	SortedRunScanState sorted_run_scan_state;
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
class SortedRunMergerGlobalState : public GlobalSourceState {
public:
	explicit SortedRunMergerGlobalState(ClientContext &context_p, const SortedRunMerger &merger_p)
	    : context(context_p), num_threads(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads())),
	      merger(merger_p), num_runs(merger.sorted_runs.size()),
	      num_partitions((merger.total_count + (merger.partition_size - 1)) / merger.partition_size),
	      iterator_state_type(GetBlockIteratorStateType(merger.external)),
	      sort_key_type(merger.sort.key_layout->GetSortKeyType()), next_partition_idx(0), total_scanned(0),
	      destroy_partition_idx(0) {
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

	void DestroyScannedData() {
		if (!merger.external) {
			return; // Only need to destroy when doing an external sort
		}

		// Have to do this under lock, but other threads don't have to wait
		unique_lock<mutex> guard(destroy_lock, std::try_to_lock);
		if (!guard.owns_lock()) {
			return;
		}

		// Check how many partitions we can destroy
		idx_t end_partition_idx;
		for (end_partition_idx = destroy_partition_idx; end_partition_idx < num_partitions; end_partition_idx++) {
			const auto &partition = *partitions[end_partition_idx];
			if (!partition.scanned) {
				break;
			}
		}

		// Destroying must lag "num_threads" partitions behind to avoid destroying actively used data
		if (end_partition_idx < num_threads) {
			return;
		}
		end_partition_idx -= num_threads;
		if (end_partition_idx <= destroy_partition_idx) {
			return;
		}

		// Compute average number of tuples per partition to destroy
		const auto total_tuples_to_destroy = (end_partition_idx - destroy_partition_idx) * merger.partition_size;
		const auto tuples_to_destroy_per_partition = total_tuples_to_destroy / num_partitions;

		// Compute max tuples per block
		const auto &run = *merger.sorted_runs[0];
		const auto tuples_per_block =
		    run.payload_data ? run.payload_data->TuplesPerBlock() : run.key_data->TuplesPerBlock();

		// Compute number of blocks we can destroy per partition
		const auto blocks_to_destroy_per_partition = tuples_to_destroy_per_partition / tuples_per_block;
		if (blocks_to_destroy_per_partition == 0) {
			return;
		}

		for (idx_t run_idx = 0; run_idx < num_runs; run_idx++) {
			idx_t begin_idx = 0;
			if (destroy_partition_idx != 0) {
				auto &begin_partition = *partitions[destroy_partition_idx];
				auto partition_guard = begin_partition.Lock();
				begin_idx = begin_partition.GetRunBoundaries(partition_guard)[run_idx].end;
			}

			idx_t end_idx = merger.sorted_runs[run_idx]->Count();
			if (end_partition_idx != num_partitions) {
				auto &end_partition = *partitions[end_partition_idx];
				auto partition_guard = end_partition.Lock();
				end_idx = end_partition.GetRunBoundaries(partition_guard)[run_idx].end;
			}

			merger.sorted_runs[run_idx]->DestroyData(begin_idx, end_idx);
		}

		destroy_partition_idx = end_partition_idx;
	}

private:
	static BlockIteratorStateType GetBlockIteratorStateType(const bool &external) {
		return external ? BlockIteratorStateType::EXTERNAL : BlockIteratorStateType::IN_MEMORY;
	}

public:
	ClientContext &context;
	const idx_t num_threads;

	const SortedRunMerger &merger;
	const idx_t num_runs;
	const idx_t num_partitions;

	const BlockIteratorStateType iterator_state_type;
	const SortKeyType sort_key_type;

	idx_t next_partition_idx;
	vector<unique_ptr<SortedRunMergePartition>> partitions;
	atomic<idx_t> total_scanned;

	mutex destroy_lock;
	idx_t destroy_partition_idx;

	mutex materialized_partition_lock;
	vector<unique_ptr<SortedRun>> materialized_partitions;
};

//===--------------------------------------------------------------------===//
// Local State Implementation
//===--------------------------------------------------------------------===//
SortedRunMergerLocalState::SortedRunMergerLocalState(SortedRunMergerGlobalState &gstate)
    : iterator_state_type(gstate.iterator_state_type), sort_key_type(gstate.sort_key_type),
      task(SortedRunMergerTask::FINISHED), run_boundaries(gstate.num_runs),
      merged_partition_count(DConstants::INVALID_INDEX), merged_partition_index(DConstants::INVALID_INDEX),
      sorted_run_scan_state(gstate.context, gstate.merger.sort), sort_key_pointers(LogicalType::POINTER) {
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

SourceResultType SortedRunMergerLocalState::ExecuteTask(SortedRunMergerGlobalState &gstate,
                                                        optional_ptr<DataChunk> chunk) {
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
		if (chunk) {
			ScanPartition(gstate, *chunk);
		} else {
			MaterializePartition(gstate);
		}
		if (!chunk || chunk->size() == 0) {
			gstate.DestroyScannedData();
			gstate.partitions[partition_idx.GetIndex()]->scanned = true;
			//	fetch_add returns the _previous_ value!
			const auto scan_count_before_adding = gstate.total_scanned.fetch_add(merged_partition_count);
			const auto scan_count_after_adding = scan_count_before_adding + merged_partition_count;
			partition_idx = optional_idx::Invalid();
			task = SortedRunMergerTask::FINISHED;
			if (scan_count_after_adding == gstate.merger.total_count) {
				return SourceResultType::FINISHED;
			}
		}
		break;
	default:
		throw NotImplementedException("SortedRunMergerLocalState::ExecuteTask for task");
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

void SortedRunMergerLocalState::ComputePartitionBoundaries(SortedRunMergerGlobalState &gstate,
                                                           const optional_idx &p_idx) {
	D_ASSERT(p_idx.IsValid());
	D_ASSERT(task == SortedRunMergerTask::COMPUTE_BOUNDARIES);

	// Copy over the run boundaries from the assigned partition (under lock)
	auto &current_partition = *gstate.partitions[p_idx.GetIndex()];
	auto current_partition_guard = current_partition.Lock();
	const auto begin_computed = current_partition.GetBeginComputed();
	run_boundaries = current_partition.GetRunBoundaries(current_partition_guard);
	current_partition_guard.unlock();

	if (!begin_computed) {
		// We can use information from previous partitions to speed up computing this partition
		for (idx_t prev = p_idx.GetIndex(); prev > 1; prev--) {
			auto &prev_partition = *gstate.partitions[prev - 1];
			if (!prev_partition.GetBeginComputed()) {
				continue;
			}
			auto prev_partition_guard = prev_partition.Lock();
			const auto &prev_partition_run_boundaries = prev_partition.GetRunBoundaries(prev_partition_guard);
			for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
				run_boundaries[run_idx].begin = prev_partition_run_boundaries[run_idx].begin;
			}
			break;
		}
	}

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
		if (!next_partition.GetBeginComputed()) {
			auto next_partition_guard = next_partition.Lock();
			if (!next_partition.GetBeginComputed()) {
				auto &next_partition_run_boundaries = next_partition.GetRunBoundaries(next_partition_guard);
				for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
					const auto &computed_boundary = run_boundaries[run_idx];
					D_ASSERT(computed_boundary.begin == computed_boundary.end);
					next_partition_run_boundaries[run_idx].begin = computed_boundary.begin;
				}
				next_partition.SetBeginComputed();
			}
		}
	}

	// Set the computed end partition boundaries of the current partition
	current_partition_guard.lock();
	auto &current_partition_run_boundaries = current_partition.GetRunBoundaries(current_partition_guard);
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
	unsafe_vector<idx_t> active_run_idxs;
	active_run_idxs.reserve(gstate.num_runs);
	for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
		auto &state = states[run_idx];
		state.SetKeepPinned(false);
		state.SetPinPayload(false);

		run_iterators.emplace_back(state);
		const auto &run_boundary = run_boundaries[run_idx];
		if (run_boundary.begin != run_boundary.end) {
			active_run_idxs.emplace_back(run_idx);
		}

		// Reduce total remaining by what we already have (from previous partition)
		D_ASSERT(run_boundaries[run_idx].begin <= total_remaining);
		total_remaining -= run_boundaries[run_idx].begin;
	}

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
	if (current_partition.GetBeginComputed()) {
		// Begin has been computed, boundaries are ready to use. Copy to local
		auto guard = current_partition.Lock();
		run_boundaries = current_partition.GetRunBoundaries(guard);
		return;
	}

	// Begin has not yet been computed by another thread, let this thread do it
	task = SortedRunMergerTask::COMPUTE_BOUNDARIES;
	ComputePartitionBoundaries(gstate, partition_idx.GetIndex() - 1);
	task = SortedRunMergerTask::ACQUIRE_BOUNDARIES;

	// Copy to local
	auto guard = current_partition.Lock();
	D_ASSERT(current_partition.GetBeginComputed());
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
		auto &state = states[run_idx];
		state.SetKeepPinned(true);
		state.SetPinPayload(true);

		// Keep track of how many runs are actively being merged
		const auto &run_boundary = run_boundaries[run_idx];
		if (run_boundary.begin == run_boundary.end) {
			continue;
		}
		active_runs++;

		for (auto it = BLOCK_ITERATOR(state, run_boundary.begin); it != BLOCK_ITERATOR(state, run_boundary.end); ++it) {
			merged_partition_keys[merged_partition_count++] = *it;
		}
	}

	if (active_runs == 1 || gstate.merger.is_index_sort) {
		return; // Only one active run, no need to sort (or index sort, which is approximate sorting)
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

	// Grab pointers to sort keys
	const auto merged_partition_keys = reinterpret_cast<SORT_KEY *>(merged_partition.get()) + merged_partition_index;
	const auto sort_keys = FlatVector::GetData<SORT_KEY *>(sort_key_pointers);
	for (idx_t i = 0; i < count; i++) {
		sort_keys[i] = &merged_partition_keys[i];
	}
	merged_partition_index += count;

	// Scan
	sorted_run_scan_state.Scan(*gstate.merger.sorted_runs[0], sort_key_pointers, count, chunk);
}

void SortedRunMergerLocalState::MaterializePartition(SortedRunMergerGlobalState &gstate) {
	unique_ptr<SortedRun> sorted_run;
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		sorted_run = TemplatedMaterializePartition<SortKeyType::NO_PAYLOAD_FIXED_8>(gstate);
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		sorted_run = TemplatedMaterializePartition<SortKeyType::NO_PAYLOAD_FIXED_16>(gstate);
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		sorted_run = TemplatedMaterializePartition<SortKeyType::NO_PAYLOAD_FIXED_24>(gstate);
		break;
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		sorted_run = TemplatedMaterializePartition<SortKeyType::NO_PAYLOAD_FIXED_32>(gstate);
		break;
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		sorted_run = TemplatedMaterializePartition<SortKeyType::NO_PAYLOAD_VARIABLE_32>(gstate);
		break;
	case SortKeyType::PAYLOAD_FIXED_16:
		sorted_run = TemplatedMaterializePartition<SortKeyType::PAYLOAD_FIXED_16>(gstate);
		break;
	case SortKeyType::PAYLOAD_FIXED_24:
		sorted_run = TemplatedMaterializePartition<SortKeyType::PAYLOAD_FIXED_24>(gstate);
		break;
	case SortKeyType::PAYLOAD_FIXED_32:
		sorted_run = TemplatedMaterializePartition<SortKeyType::PAYLOAD_FIXED_32>(gstate);
		break;
	case SortKeyType::PAYLOAD_VARIABLE_32:
		sorted_run = TemplatedMaterializePartition<SortKeyType::PAYLOAD_VARIABLE_32>(gstate);
		break;
	default:
		throw NotImplementedException("SortedRunMergerLocalState::MaterializePartition for %s",
		                              EnumUtil::ToString(sort_key_type));
	}

	// Add to global state
	lock_guard<mutex> guard(gstate.materialized_partition_lock);
	if (gstate.materialized_partitions.size() < partition_idx.GetIndex() + 1) {
		gstate.materialized_partitions.resize(partition_idx.GetIndex() + 1);
	}
	gstate.materialized_partitions[partition_idx.GetIndex()] = std::move(sorted_run);
}

template <SortKeyType SORT_KEY_TYPE>
unique_ptr<SortedRun> SortedRunMergerLocalState::TemplatedMaterializePartition(SortedRunMergerGlobalState &gstate) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	const auto merged_partition_keys = reinterpret_cast<SORT_KEY *>(merged_partition.get()) + merged_partition_index;

	TupleDataChunkState key_data_input;
	const auto key_locations = FlatVector::GetData<data_ptr_t>(key_data_input.row_locations);
	const auto key_heap_locations = FlatVector::GetData<data_ptr_t>(key_data_input.heap_locations);
	const auto key_heap_sizes = FlatVector::GetData<idx_t>(key_data_input.heap_sizes);

	TupleDataChunkState payload_data_input;
	const auto payload_locations = FlatVector::GetData<data_ptr_t>(payload_data_input.row_locations);

	auto sorted_run = gstate.merger.sorted_runs[0]->CreateRunForMaterialization();

	while (merged_partition_index < merged_partition_count) {
		const auto count = MinValue<idx_t>(merged_partition_count - merged_partition_index, STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			auto &key = merged_partition_keys[merged_partition_index + i];
			key_locations[i] = data_ptr_cast(&key);
			if (!SORT_KEY::CONSTANT_SIZE) {
				key_heap_locations[i] = key.GetData();
				key_heap_sizes[i] = key.GetHeapSize();
			}
			if (SORT_KEY::HAS_PAYLOAD) {
				payload_locations[i] = key.GetPayload();
			}
		}

		sorted_run->key_append_state.chunk_state.heap_sizes.Reference(key_data_input.heap_sizes);
		sorted_run->key_data->Build(sorted_run->key_append_state.pin_state, sorted_run->key_append_state.chunk_state, 0,
		                            count);
		sorted_run->key_data->CopyRows(sorted_run->key_append_state.chunk_state, key_data_input,
		                               *FlatVector::IncrementalSelectionVector(), count);

		if (SORT_KEY::HAS_PAYLOAD) {
			if (!sorted_run->payload_data->GetLayout().AllConstant()) {
				sorted_run->payload_data->FindHeapPointers(payload_data_input, count);
			}
			sorted_run->payload_append_state.chunk_state.heap_sizes.Reference(payload_data_input.heap_sizes);
			sorted_run->payload_data->Build(sorted_run->payload_append_state.pin_state,
			                                sorted_run->payload_append_state.chunk_state, 0, count);
			sorted_run->payload_data->CopyRows(sorted_run->payload_append_state.chunk_state, payload_data_input,
			                                   *FlatVector::IncrementalSelectionVector(), count);
		}

		merged_partition_index += count;
	}

	sorted_run->key_data->FinalizePinState(sorted_run->key_append_state.pin_state);
	if (sorted_run->payload_data) {
		sorted_run->payload_data->FinalizePinState(sorted_run->payload_append_state.pin_state);
	}

	return sorted_run;
}

//===--------------------------------------------------------------------===//
// Sorted Run Merger
//===--------------------------------------------------------------------===//
SortedRunMerger::SortedRunMerger(const Sort &sort_p, vector<unique_ptr<SortedRun>> &&sorted_runs_p,
                                 idx_t partition_size_p, bool external_p, bool is_index_sort_p)
    : sort(sort_p), sorted_runs(std::move(sorted_runs_p)), total_count(SortedRunsTotalCount(sorted_runs)),
      partition_size(partition_size_p), external(external_p), is_index_sort(is_index_sort_p) {
}

unique_ptr<LocalSourceState> SortedRunMerger::GetLocalSourceState(ExecutionContext &,
                                                                  GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<SortedRunMergerGlobalState>();
	auto guard = gstate.Lock();
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
			lstate.ExecuteTask(gstate, &chunk);
		} else {
			break;
		}
	}

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

//===--------------------------------------------------------------------===//
// Non-Standard Interface
//===--------------------------------------------------------------------===//
SourceResultType SortedRunMerger::MaterializeSortedRun(ExecutionContext &, OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<SortedRunMergerGlobalState>();
	auto &lstate = input.local_state.Cast<SortedRunMergerLocalState>();

	SourceResultType res = SourceResultType::HAVE_MORE_OUTPUT;
	while (true) {
		if (!lstate.TaskFinished() || gstate.AssignTask(lstate)) {
			res = lstate.ExecuteTask(gstate, nullptr);
		} else {
			break;
		}
	}

	// The thread that completes the materialization returns FINISHED, all other threads return HAVE_MORE_OUTPUT
	return res;
}

unique_ptr<SortedRun> SortedRunMerger::GetSortedRun(GlobalSourceState &global_state) {
	auto &gstate = global_state.Cast<SortedRunMergerGlobalState>();
	D_ASSERT(total_count != 0);
	lock_guard<mutex> guard(gstate.materialized_partition_lock);
	if (gstate.materialized_partitions.empty()) {
		return nullptr;
	}
	auto &target = *gstate.materialized_partitions[0];
	for (idx_t i = 1; i < gstate.materialized_partitions.size(); i++) {
		auto &source = *gstate.materialized_partitions[i];
		target.key_data->Combine(*source.key_data);
		if (target.payload_data) {
			target.payload_data->Combine(*source.payload_data);
		}
	}
	auto res = std::move(gstate.materialized_partitions[0]);
	gstate.materialized_partitions.clear();
	return res;
}

} // namespace duckdb
