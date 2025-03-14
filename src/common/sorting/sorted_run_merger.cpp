#include "duckdb/common/sorting/sorted_run_merger.hpp"

#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/sorting/tournament_tree.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sorted Run Merger Utility
//===--------------------------------------------------------------------===//
static idx_t SortedRunsTotalCount(const vector<unique_ptr<SortedRun>> &sorted_runs) {
	idx_t total_count = 0;
	for (const auto &sorted_run : sorted_runs) {
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
	void ComputePartitionBoundaries(SortedRunMergerGlobalState &gstate);
	template <class STATE>
	void ComputePartitionBoundariesSwitch(SortedRunMergerGlobalState &gstate, unsafe_vector<STATE> &states);
	template <class STATE, SortKeyType SORT_KEY_TYPE>
	void TemplatedComputePartitionBoundaries(SortedRunMergerGlobalState &gstate, unsafe_vector<STATE> &states);
	//! Acquires lower partition boundaries from the global state
	bool AcquirePartitionBoundaries(SortedRunMergerGlobalState &gstate);
	//! Acquire the next chunk
	void MergePartition(SortedRunMergerGlobalState &gstate, DataChunk &chunk);

public:
	SortedRunMergerTask task;
	optional_idx partition_idx;

private:
	//! Computed run boundaries
	unsafe_vector<SortedRunPartitionBoundary> run_boundaries;

	//! States for every iterator type
	unsafe_vector<const fixed_in_memory_block_iterator_state_t> fixed_in_memory_states;
	//! TODO: other iterator type
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
class SortedRunMergerGlobalState : public GlobalSourceState {
public:
	explicit SortedRunMergerGlobalState(const SortedRunMerger &merger_p)
	    : merger(merger_p), num_runs(merger.sorted_runs.size()),
	      num_partitions((merger.total_count + (merger.partition_size - 1)) / merger.partition_size),
	      iterator_state_type(GetBlockIteratorStateType(merger.fixed_blocks, merger.external)), next_partition_idx(0) {
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
		return true;
	}

	idx_t MaxThreads() override {
		return num_partitions;
	}

public:
	const SortedRunMerger &merger;
	const idx_t num_runs;
	const idx_t num_partitions;
	const BlockIteratorStateType iterator_state_type;

	idx_t next_partition_idx;
	vector<unique_ptr<SortedRunMergePartition>> partitions;
};

//===--------------------------------------------------------------------===//
// Local State Implementation
//===--------------------------------------------------------------------===//
SortedRunMergerLocalState::SortedRunMergerLocalState(SortedRunMergerGlobalState &gstate)
    : task(SortedRunMergerTask::FINISHED), run_boundaries(gstate.num_runs) {
	for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
		fixed_in_memory_states.emplace_back(*gstate.merger.sorted_runs[run_idx]->key_data);
	}
}

bool SortedRunMergerLocalState::TaskFinished() const {
	switch (task) {
	case SortedRunMergerTask::COMPUTE_BOUNDARIES:
	case SortedRunMergerTask::ACQUIRE_BOUNDARIES:
	case SortedRunMergerTask::MERGE_PARTITION:
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
		ComputePartitionBoundaries(gstate);
		task = SortedRunMergerTask::ACQUIRE_BOUNDARIES;
		break;
	case SortedRunMergerTask::ACQUIRE_BOUNDARIES:
		if (AcquirePartitionBoundaries(gstate)) {
			task = SortedRunMergerTask::MERGE_PARTITION;
		}
		break;
	case SortedRunMergerTask::MERGE_PARTITION:
		MergePartition(gstate, chunk);
		if (chunk.size() == 0) {
			task = SortedRunMergerTask::FINISHED;
			partition_idx = optional_idx::Invalid();
		}
		break;
	default:
		throw NotImplementedException("SortedRunMergerLocalState::ExecuteTask for task");
	}
}

void SortedRunMergerLocalState::ComputePartitionBoundaries(SortedRunMergerGlobalState &gstate) {
	D_ASSERT(partition_idx.IsValid());
	D_ASSERT(task == SortedRunMergerTask::COMPUTE_BOUNDARIES);

	// Copy over the run boundaries from the assigned partition (under lock)
	auto &current_partition = *gstate.partitions[partition_idx.GetIndex()];
	auto guard = current_partition.Lock();
	run_boundaries = current_partition.GetRunBoundaries(guard);
	guard.unlock();

	// Compute the end partition boundaries (lock-free)
	switch (gstate.iterator_state_type) {
	case BlockIteratorStateType::FIXED_IN_MEMORY:
		ComputePartitionBoundariesSwitch<const block_iterator_state_t<BlockIteratorStateType::FIXED_IN_MEMORY>>(
		    gstate, fixed_in_memory_states);
		break;
	default:
		// TODO: switch on other types
		throw NotImplementedException(
		    "SortedRunMergerLocalState::ComputePartitionBoundaries for SortedRunIteratorType");
	}

	// The computed boundaries of the current partition may be the start boundaries of the next partition
	// Another thread depends on this, set them first
	if (partition_idx.GetIndex() != gstate.num_partitions - 1) {
		auto &next_partition = *gstate.partitions[partition_idx.GetIndex() + 1];
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

	// Next task is to acquire the boundaries of the previous partition
	task = SortedRunMergerTask::ACQUIRE_BOUNDARIES;
}

template <class STATE>
void SortedRunMergerLocalState::ComputePartitionBoundariesSwitch(SortedRunMergerGlobalState &gstate,
                                                                 unsafe_vector<STATE> &states) {
	const auto sort_key_type = gstate.merger.sorted_runs[0]->key_data->GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_8>(gstate, states);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_16>(gstate, states);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_24>(gstate, states);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_FIXED_32>(gstate, states);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::NO_PAYLOAD_VARIABLE_32>(gstate, states);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_FIXED_16>(gstate, states);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_FIXED_24>(gstate, states);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_FIXED_32>(gstate, states);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedComputePartitionBoundaries<STATE, SortKeyType::PAYLOAD_VARIABLE_32>(gstate, states);
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
                                                                    unsafe_vector<STATE> &states) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using ITER = block_iterator_t<STATE, SORT_KEY>;

	D_ASSERT(run_boundaries.size() == gstate.num_runs);

	// Check if last partition: boundary is always end of each sorted run
	if (partition_idx == gstate.num_partitions - 1) {
		for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
			run_boundaries[run_idx].begin = gstate.merger.sorted_runs[run_idx]->Count();
		}
		return;
	}

	// Initialize iterators, and track of which runs are actively being used in the computation, i.e., not yet fixed
	unsafe_vector<ITER> run_iterators;
	run_iterators.reserve(gstate.num_runs);
	unsafe_vector<idx_t> active_run_idxs(gstate.num_runs);
	for (idx_t run_idx = 0; run_idx < gstate.num_runs; run_idx++) {
		run_iterators.emplace_back(states[run_idx]);
		active_run_idxs[run_idx] = run_idx;
	}

	// Deltas per run, used for convenience below
	unsafe_vector<idx_t> run_deltas(gstate.num_runs);

	// Initialize "total_remaining", i.e., how much we still need to update the boundaries until we're done
	idx_t total_remaining = (partition_idx.GetIndex() + 1) * gstate.merger.partition_size;
	D_ASSERT(total_remaining <
	         gstate.merger.total_count); // This is not the last partition, so should be less than total
	while (total_remaining != 0) {
		const idx_t k = active_run_idxs.size();

		// Compute step size for each run: ceil(total_remaining / k), or less if run has less remaining
		auto min_idx = active_run_idxs[0];
		auto min_delta = ComputeBoundaryDelta(total_remaining, k, run_boundaries[min_idx]);
		reference<SORT_KEY> min_value = run_iterators[min_idx][run_boundaries[min_idx].begin + min_delta];
		for (idx_t i = 1; i < active_run_idxs.size(); i++) {
			const auto &active_run_idx = active_run_idxs[i];
			const auto &run_boundary = run_boundaries[active_run_idx];
			const auto run_delta = ComputeBoundaryDelta(total_remaining, k, run_boundary);
			auto &run_value = run_iterators[active_run_idx][run_boundary.begin + run_delta];
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
		D_ASSERT(min_delta >= total_remaining);
		total_remaining -= min_delta;
	}

	// end of boundary is meaningless now, just set equal to begin so there's no confusion
	for (auto &run_boundary : run_boundaries) {
		run_boundary.end = run_boundary.begin;
	}
}

bool SortedRunMergerLocalState::AcquirePartitionBoundaries(SortedRunMergerGlobalState &gstate) {
	D_ASSERT(partition_idx.IsValid());
	D_ASSERT(task == SortedRunMergerTask::ACQUIRE_BOUNDARIES);
	auto &current_partition = *gstate.partitions[partition_idx.GetIndex()];
	auto guard = current_partition.Lock();
	if (!current_partition.GetStartComputed(guard)) {
		return false; // Start has not yet been computed
	}

	// Start has been computed, boundaries are ready to use. Copy to local
	run_boundaries = current_partition.GetRunBoundaries(guard);
	return true;
}

void SortedRunMergerLocalState::MergePartition(SortedRunMergerGlobalState &gstate, DataChunk &chunk) {
	throw NotImplementedException("Sort");
}

//===--------------------------------------------------------------------===//
// Sorted Run Merger
//===--------------------------------------------------------------------===//
SortedRunMerger::SortedRunMerger(vector<unique_ptr<SortedRun>> &&sorted_runs_p, idx_t partition_size_p, bool external_p,
                                 bool fixed_blocks_p)
    : sorted_runs(std::move(sorted_runs_p)), total_count(SortedRunsTotalCount(sorted_runs)),
      partition_size(partition_size_p), external(external_p), fixed_blocks(fixed_blocks_p) {
	D_ASSERT(total_count != 0);
}

unique_ptr<LocalSourceState> SortedRunMerger::GetLocalSourceState(ExecutionContext &,
                                                                  GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<SortedRunMergerGlobalState>();
	return make_uniq<SortedRunMergerLocalState>(gstate);
}

unique_ptr<GlobalSourceState> SortedRunMerger::GetGlobalSourceState(ClientContext &) const {
	return make_uniq<SortedRunMergerGlobalState>(*this);
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

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

OperatorPartitionData SortedRunMerger::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                        GlobalSourceState &gstate, LocalSourceState &lstate,
                                                        const OperatorPartitionInfo &partition_info) const {
	throw NotImplementedException("Sort");
}

ProgressData SortedRunMerger::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	throw NotImplementedException("Sort");
}

} // namespace duckdb
