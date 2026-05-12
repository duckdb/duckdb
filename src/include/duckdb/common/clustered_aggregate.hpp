//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/clustered_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

class Vector;

//! Per-chunk tuple clustering by group. Built once and passed to aggregate kernels.
//! Clustered-aware kernels can use per-run accumulation; everyone else keeps the
//! regular scatter path over input-order addresses.
struct ClusteredAggr {
	static constexpr idx_t RUNLENGTH_THRESHOLD = 6;
	static constexpr idx_t MAX_RUNS = STANDARD_VECTOR_SIZE;
	static constexpr idx_t HOTKEYS_LOG2 = 5;
	static constexpr idx_t MAX_HOTKEYS = idx_t(1) << HOTKEYS_LOG2;
	static constexpr idx_t SAMPLE_SIZE = 128;
	static constexpr idx_t HASHTAB_LOG2 = 11;
	static constexpr idx_t HASHTAB_SZ = idx_t(1) << HASHTAB_LOG2;
	static constexpr idx_t SLOT_GRP_BITS = 13;
	static constexpr idx_t SLOT_CURSOR_BITS = (HOTKEYS_LOG2 + 1) + SLOT_GRP_BITS;
	static constexpr idx_t SLOT_GID_BITS = 64 - (SLOT_GRP_BITS + SLOT_CURSOR_BITS);
	static constexpr idx_t MAX_GID_COUNT = idx_t(1) << SLOT_GID_BITS;
	static constexpr uint64_t FREE_SLOT = ~uint64_t(0);

	struct GroupRun {
		data_ptr_t state; //! caller fills this after TryClustered; advanced between aggregates
		const sel_t *sel; //! points to the tuple positions for this run
		uint64_t gid;     //! raw group id for this run
		idx_t count;      //! number of tuples in this group
	};

	idx_t n_group_runs = 0;
	GroupRun group_runs[MAX_RUNS];

	//! Build a clustered permutation of 0..count-1 from raw integer group ids.
	//! On success fills group_runs[].sel/gid/count.
	bool TryClustered(const uint64_t *group_ids, sel_t count, sel_t *arena, uint64_t *slots);

	//! Initialize a single run covering 0..count-1 for one aggregate state.
	void SetSingleRun(data_ptr_t state, idx_t count);

	//! Advance all run state pointers by payload_size.
	void AdvanceStates(idx_t payload_size);

	template <class GET_STATE>
	void InitializeStates(GET_STATE &&get_state) {
		for (idx_t r = 0; r < n_group_runs; r++) {
			group_runs[r].state = get_state(group_runs[r].gid);
		}
	}

	//! Returns a composed dict sel for simple dictionary input, or nullptr.
	const sel_t *ClusterIter(const Vector &input, idx_t count) const;

private:
	mutable sel_t composed_sel_data[STANDARD_VECTOR_SIZE];
	mutable const sel_t *cached_dict_sel = nullptr;
};

//! Scratch state shared by GroupedAggregateHashTable and PerfectAggregateHashTable.
struct ClusteredAggrState {
	unsafe_unique_array<sel_t> arena;
	unsafe_unique_array<uint64_t> slots;
	bool all_clustered = false;
	idx_t n_clustered = 0;
	idx_t skipped_opportunities = 0;
	idx_t retry_backoff = 1;

	void Initialize();
	bool TryBuild(ClusteredAggr &clustered, const uint64_t *group_ids, idx_t count);
};

} // namespace duckdb
