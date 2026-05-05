#include "duckdb/common/clustered_aggr.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

// Why does clustering help performance so much in TPC-H q01? Yes, "in-register" sum (for example) is
// faster than load-add-store and avoids setting "bool is_set" every time - but this is the sub-plot only.
// The main reason is cache-consistency latency because multiple load-add-store patterns hit the
// same aggregate state in short succession. To illustrate that with TPC-H Q01, change SELECT
// l_returnflag,l_shipmode into SELECT CAST(l_quantity * 1.0 AS INT) .. GROUP BY ALL ORDER BY ALL.
// With the 1.0 constant there are 50 groups, but by decreasing it, one can scale flexibly down to
// fewer groups. Negative performance effects start around 25 and grow strongest at <= 5 groups.
//
// Is this only affecting aggregations with <25 groups? No, there are two more cases:
// - (very) heavy hittng keys in a GROUP BY with potentially many groups
//   e.g. 50% 0: SELECT (rowid&1)*rowid, sum(l_extendedprice) FROM  lineitem GROUP BY ALL;
// - repeating keys: rolling up an ordered dataframe, or aggregating back to the PK of a FK-PK join.
//   e.g. SELECT l_orderkey, sum(l_extendedprice) FROM lineitem GROUP BY ALL;

// the conclusion of this is that we pursue a dual strategy:
// - try to construct complete, ordered, position lists for each unique group, until we found 32
//   rationale: we know that with more than 32 frequent groups, there will be no benefit
// - continue nevertheless but only detect sequential GID runs (or GIDs already in the hash table)
//   rationale: sequential runs, even when on average short, are also valuable to detect
//
// if in the end the amount of runs detected is < 1/3 of the input, we declare success
// - this is trivially the case if there are < 32 groups (assuming at least 96 input tuples)
// - this is also the case if the input is clustered and the avg repeat rate is >3
// - it will also detect non-sequential high-hitters that dominate (67%) the input 
// 
// in the code we use MAX_GROUPS=32 and MIN_AVG_RUNLENGTH=3

bool ClusteredAggr::TryClustered(const uint64_t *group_ids, idx_t count, sel_t *arena, uint32_t *slots) {
	sel_t *left_list_cursor[MAX_RUNS];
	sel_t *right_list_cursor[MAX_RUNS];
	idx_t left_run_cursor = FIRST_GROUP;
	idx_t right_run_cursor = FIRST_GROUP;
	idx_t cur_num_groups = 0; 
	
	n_group_runs = 0;

	auto find_group = [&](uint64_t gid, idx_t &slot) -> GroupRun * {
		slot = gid & (HASH_SLOTS - 1);
		if (DUCKDB_LIKELY((slots[slot] & GID_MASK) == gid)) {
			return &group_runs[FIRST_GROUP + (slots[slot] >> POSITION_SHIFT)];
		}
		return nullptr;
	};

	auto find_group_or_run = [&](uint64_t gid, idx_t &slot, uint64_t prev_gid, GroupRun *prev_group) -> GroupRun * {
		// we detect sequential runs, but also try the hash table (to find high-frequency hitting gids)
		return (gid == prev_gid) ? prev_group : find_group(gid, slot);  // ..otherwise we just detect sequential gid "runs"
	};

	auto append_group = [&](uint64_t gid, idx_t slot) -> GroupRun * {
		if (cur_num_groups == MAX_GROUPS || slots[slot] != INVALID_SLOT) { // full or collision
    			right_run_cursor = FIRST_GROUP + cur_num_groups; // initialize right_run_cursor (starts right after the hash-groups)
			return nullptr;
		}
		idx_t group_pos = FIRST_GROUP + cur_num_groups++; // early increment: 1st vector is for sequential runs (non-hashed)
		group_runs[group_pos].gid = gid;
		auto list_center = arena + cur_num_groups * STANDARD_VECTOR_SIZE + STANDARD_VECTOR_SIZE / 2;
		left_list_cursor[group_pos] = right_list_cursor[group_pos] = list_center;

                // insert into the mini-hash table 
		slots[slot] = static_cast<uint32_t>(gid) | (static_cast<uint32_t>(group_pos - FIRST_GROUP) << POSITION_SHIFT);
		return &group_runs[group_pos];
	};

        // group_runs[], but also left/right_list_cursor[] arrays have the following layout:
        // 1- some free space, such that -3- alwasy starts at FIRST_GROUP
        // 2- (up to) 2*STANDARD_VECTOR_SIZE/3 (3=MIN_AVG_RUNLENGTH) left "runs"
        // 3- (up to) MAX_GROUPS (32) hash exact "groups"
        // 4- (up to) 2*STANDARD_VECTOR_SIZE/3 right "runs"
        // 5- some free space
        //
        //  0       left_run_cursor         FIRST_GROUP      FIRST_GROUP+cur_num_groups       right_run_cursor
        //  |          |                         | (fixed)          | (cur_num_groups<MAX_GROUPS) | 
        //  +----------+-------------------------+------------------+-----------------------------+---------+
        //  | free     | GroupRun records        | GroupRun records | GroupRun records            | free    |
        //  +----------+-------------------------+------------------+-----------------------------+---------+
        //               sequential "right runs"   exact "groups"        sequential "left runs"   
        //
        // each group/run has a "list" with *all* positions (the .sel field) for that GID in the run (ordered),
        //
        // either "group" or "run":
        // - the cur_num_groups exact "groups" each have a full STANDARD_VECTOR_SIZE vector from the arena
        //                                  <=|= found by left| found by right cursor  =|=>    
        //   vector of sel_t: +---------------+---------------+-------------------------|-------------|
        //    (32 of these    |   free        |         sorted positions                | free        |
        //     in arena)      +---------------+---------------+-------------------------|-------------|
        //                    |               |               |                         |             |
        //                    0  <=left_list_cursor[x]    center/startpos   right_list_cursor[x]=> 2048
        // or:
        // - the left/right "runs" put their positionn lists data in one SINGLE vector (the first in the arena) 
        //   because they are found sequentially and their total size cannot exceed half the vectorsize
        //   the left runs only update the left_list_cursor[y] and the right update right_list_cursor[y]
        //   the length of each run is the subtraction: right_list_cursor[y+1] - right_list_cursor[y]     
        //
        // because these list cursors are also used for the (up to) 32 hashed "groups" in the middle
        // we need to be careful with the first left and right borders, hence LEFT_LIST_END/RIGHT_LIST_END

#define LEFT_LIST_END(cur) ((cur+1 == FIRST_GROUP) ? (arena + STANDARD_VECTOR_SIZE) : left_list_cursor[cur+1])
#define RIGHT_LIST_START(cur) ((cur == FIRST_GROUP + cur_num_groups) ? arena : right_list_cursor[cur-1])

	auto append_run_left = [&](uint64_t gid, idx_t &slot) -> GroupRun * {
		group_runs[--left_run_cursor].gid = gid;
		left_list_cursor[left_run_cursor] = LEFT_LIST_END(left_run_cursor);
		return &group_runs[left_run_cursor];
	};

	auto append_run_right = [&](uint64_t gid, idx_t &slot) -> GroupRun * {
		group_runs[right_run_cursor].gid = gid;
		right_list_cursor[right_run_cursor] = RIGHT_LIST_START(right_run_cursor);
		return &group_runs[right_run_cursor++];
	};

	auto cleanup = [&](bool status) {
		for (idx_t i = 0; i < cur_num_groups; i++) { // clear used slots in the hash table
			slots[group_runs[FIRST_GROUP + i].gid & (HASH_SLOTS - 1)] = INVALID_SLOT;
		}
		cached_dict_sel = nullptr;
		return status;
	};

	// Do the assignment of a vector of GIDs to runs (a list of positions with the same GID) using "double-cursor" 
        // for higher IPC. The  "cur_left" index moves from the middle of the input to the start (backwards), 
        // "cur_right" moves from the middle to the end. We also construct the position lists with 2 cursors that start 
        // at the same spot and move left resp. right (produces ordered lists)
        // invariants: 1. for each GID, its position list is ordered 
        //             2. if a GID has multiple lists, all positions in an earlier list are smaller than in its next list
        // these invariants ensure that an aggregate will process all values of one GID in the original order, 
        // when processing the GrouRuns in order 
        //
        // note that a GID can have multiple lists (runs) because we give up on hashing after detecting 32 (MAX_GROUPS) 
        // GIDs or after a hash collision. Well, hashing -- it is just modulo GID. 
        // From that moment on, we just detect sequential runs in the input

	// start by appending only "groups" (creating exact ordered posting lists, using a mini-hash table)
	const idx_t half = count / 2;
	idx_t slot_left, slot_right, base = half;
        for(idx_t i = 0; i < half; i++) {
		const idx_t cur_left = half - 1 - i;
		const idx_t cur_right = half + i;
		auto gid_left = group_ids[cur_left];
		auto gid_right = group_ids[cur_right];
		auto *gl = find_group(gid_left, slot_left);
		auto *gr = find_group(gid_right, slot_right);
                if (DUCKDB_UNLIKELY(!gl || !gr)) { 
			if (!gl) {
				gl = append_group(gid_left, slot_left);
			}
                	if (!gr) {
				gr = append_group(gid_right, slot_right);
			}
                	if (DUCKDB_UNLIKELY(!gl || !gr)) {
				base = i;
				break;
			}
		}
		auto left_group = static_cast<idx_t>(gl - group_runs);
		auto right_group = static_cast<idx_t>(gr - group_runs);
		left_list_cursor[left_group]--;
		left_list_cursor[left_group][0] = static_cast<sel_t>(cur_left);
		right_list_cursor[right_group][0] = static_cast<sel_t>(cur_right);
		right_list_cursor[right_group]++;

	}
	// continue, now appending only "runs" by looking at the previous gid (&detecting earlier "groups" in the hash table)
	right_run_cursor = FIRST_GROUP + cur_num_groups;
	uint64_t prev_left_gid = ~uint64_t(0);
	uint64_t prev_right_gid = ~uint64_t(0);
	GroupRun *prev_left_group = nullptr;
	GroupRun *prev_right_group = nullptr;
        for(idx_t i = base; i < half; i++) {
		const idx_t cur_left = half - 1 - i;
		const idx_t cur_right = half + i;
		auto gid_left = group_ids[cur_left];
		auto gid_right = group_ids[cur_right];
		auto *gl = find_group_or_run(gid_left, slot_left, prev_left_gid, prev_left_group);
		auto *gr = find_group_or_run(gid_right, slot_right, prev_right_gid, prev_right_group);
                if (DUCKDB_UNLIKELY(!gl || !gr)) { 
			if (!gl) {
				gl = append_run_left(gid_left, slot_left);
			}
                	if (!gr) {
				gr = append_run_right(gid_right, slot_right);
			}
			if (DUCKDB_UNLIKELY(MIN_AVG_RUNLENGTH*(right_run_cursor - left_run_cursor) >= count)) {
				return cleanup(false); // we consider it failure if the avg runlength is too short 
			}
		}
		auto left_group = static_cast<idx_t>(gl - group_runs);
		auto right_group = static_cast<idx_t>(gr - group_runs);
		left_list_cursor[left_group]--;
		left_list_cursor[left_group][0] = static_cast<sel_t>(cur_left);
		right_list_cursor[right_group][0] = static_cast<sel_t>(cur_right);
		right_list_cursor[right_group]++;

		prev_left_gid = gid_left;
		prev_left_group = gl;
		prev_right_gid = gid_right;
		prev_right_group = gr;
	}
        // winddown: handle the last odd value (due to double cursor)
	const idx_t cur_right = half + half;
	if (cur_right < count) { 
		auto gid_right = group_ids[cur_right];
		auto *gr = find_group_or_run(gid_right, slot_right, prev_right_gid, prev_right_group);
                if (!gr) {
			gr = append_run_right(gid_right, slot_right);
		}
		auto right_group = static_cast<idx_t>(gr - group_runs);
		right_list_cursor[right_group][0] = static_cast<sel_t>(cur_right);
		right_list_cursor[right_group]++;
	}

        // postprocessing: set the .count and start pointer (.sel) of the RunGroups, compacted to start at 0
	idx_t dst = 0;
	for(auto cur = left_run_cursor; cur < FIRST_GROUP; cur++, dst++) { // Left Runs
		group_runs[dst].gid = group_runs[cur].gid;
		group_runs[dst].sel = left_list_cursor[cur];
		group_runs[dst].count = static_cast<idx_t>(LEFT_LIST_END(cur) - left_list_cursor[cur]);
	}
	for(auto cur = FIRST_GROUP; cur < FIRST_GROUP + cur_num_groups; cur++, dst++) { // exact/hashed Groups
		group_runs[dst].gid = group_runs[cur].gid;
		group_runs[dst].sel = left_list_cursor[cur];
		group_runs[dst].count = static_cast<idx_t>(right_list_cursor[cur] - left_list_cursor[cur]);
	}
	for(auto cur = FIRST_GROUP + cur_num_groups; cur < right_run_cursor; cur++, dst++) { // Right Runs
		group_runs[dst].gid = group_runs[cur].gid;
		group_runs[dst].sel = RIGHT_LIST_START(cur);
		group_runs[dst].count = static_cast<idx_t>(right_list_cursor[cur] - RIGHT_LIST_START(cur));
	}
	n_group_runs = dst;
	run_begin = 0;

	return cleanup(true); 
}

void ClusteredAggr::SetSingleRun(data_ptr_t state, idx_t count) {
	n_group_runs = 1;
	group_runs[0].state = state;
	group_runs[0].sel = nullptr;
	group_runs[0].gid = 0;
	group_runs[0].count = count;
	cached_dict_sel = nullptr;
}

void ClusteredAggr::AdvanceStates(idx_t payload_size) {
	for (idx_t r = 0; r < n_group_runs; r++) {
		group_runs[run_begin + r].state += payload_size;
	}
}

const sel_t *ClusteredAggr::ClusterIter(const Vector &input, idx_t count) const {
	if (input.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return nullptr;
	}
	auto &child = DictionaryVector::Child(input);
	if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
		return nullptr;
	}
	auto &dict_sel = DictionaryVector::SelVector(input);
	const sel_t *dict_data = dict_sel.data();
	if (dict_data == nullptr) {
		return nullptr;
	}
	if (cached_dict_sel == dict_data) {
		return composed_sel_data;
	}
	idx_t pos = 0;
	for (idx_t r = 0; r < n_group_runs; r++) {
		const auto *run_sel = group_runs[run_begin + r].sel;
		const auto run_count = group_runs[run_begin + r].count;
		for (idx_t k = 0; k < run_count; k++) {
			auto idx = run_sel ? run_sel[k] : k;
			composed_sel_data[pos + k] = dict_data[idx];
		}
		pos += run_count;
	}
	cached_dict_sel = dict_data;
	return composed_sel_data;
}

void ClusteredAggrState::Initialize() {
	arena = make_unsafe_uniq_array_uninitialized<sel_t>((ClusteredAggr::MAX_GROUPS + 1) * STANDARD_VECTOR_SIZE);
	slots = make_unsafe_uniq_array_uninitialized<uint32_t>(ClusteredAggr::HASH_SLOTS);
	std::fill_n(slots.get(), ClusteredAggr::HASH_SLOTS, ClusteredAggr::INVALID_SLOT);
	skipped_opportunities = 0;
	retry_backoff = 1;
}

bool ClusteredAggrState::TryBuild(ClusteredAggr &clustered, const uint64_t *group_ids, idx_t count) {
	if (!arena) {
		return false;
	}
	if (skipped_opportunities > 0) {
		skipped_opportunities--;
		return false;
	}
	if (clustered.TryClustered(group_ids, count, arena.get(), slots.get())) {
		skipped_opportunities = 0;
		retry_backoff = 1;
		return true;
	}
	skipped_opportunities = retry_backoff;
	retry_backoff = MinValue<idx_t>(NumericLimits<idx_t>::Maximum() / 2, retry_backoff) * 2;
	return false;
}
} // namespace duckdb
