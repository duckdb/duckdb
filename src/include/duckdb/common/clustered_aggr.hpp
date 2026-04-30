//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/clustered_aggr.hpp
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
	static constexpr idx_t MAX_GROUPS = 256;

	struct GroupRun {
		data_ptr_t state; //! caller fills this after TryClustered; advanced between aggregates
		const sel_t *sel; //! points to the tuple positions for this run
		idx_t count;      //! number of tuples in this group
	};

	idx_t n_group_runs = 0;
	GroupRun group_runs[MAX_GROUPS];
	uint16_t group_id_per_run[MAX_GROUPS]; //! raw group id, in the order runs appear in group_runs[]

	//! Build a clustered permutation of 0..count-1 from raw integer group ids.
	//! On success fills group_runs[].sel/count and group_id_per_run[].
	//! Requires scratch buffers: arena (MAX_GROUPS * STANDARD_VECTOR_SIZE sel_t),
	//! left_cursor and right_cursor (n_groups pointers each, pre-initialized to nullptr).
	bool TryClustered(const uint64_t *group_ids, idx_t count, sel_t *arena, sel_t **left_cursor, sel_t **right_cursor);
	//! Initialize a single run covering 0..count-1 for one aggregate state.
	void SetSingleRun(data_ptr_t state, idx_t count);

	//! Advance all run state pointers by payload_size.
	void AdvanceStates(idx_t payload_size);

	//! Returns a composed dict sel for simple dictionary input, or nullptr.
	const sel_t *ClusterIter(const Vector &input, idx_t count) const;

private:
	mutable sel_t composed_sel_data[STANDARD_VECTOR_SIZE];
	mutable const sel_t *cached_dict_sel = nullptr;
};

//! Scratch state shared by GroupedAggregateHashTable and PerfectAggregateHashTable.
struct ClusteredAggregateState {
	unsafe_unique_array<sel_t> arena;
	unsafe_unique_array<sel_t *> left_cursor;
	unsafe_unique_array<sel_t *> right_cursor;
	bool all_clustered = false;
	bool any_clustered = false;

	void Initialize(idx_t n_groups);
	bool TryBuild(ClusteredAggr &clustered, const uint64_t *group_ids, idx_t count);
};

} // namespace duckdb
