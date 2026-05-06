#include "duckdb/common/clustered_aggr.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include <cstdio>

namespace {
struct ClusteredTryBuildDebugStats {
	idx_t no_arena = 0;
	idx_t backoff_skips = 0;
	idx_t trybuild_attempts = 0;
	idx_t trybuild_successes = 0;
	idx_t trybuild_failures = 0;
	idx_t trycluster2_calls = 0;
	idx_t trycluster2_successes = 0;
	idx_t trycluster2_fail_group_limit = 0;
	idx_t trycluster2_fail_collision = 0;
	idx_t trycluster2_rows_success = 0;
	idx_t trycluster2_rows_fail = 0;
	idx_t tc_calls = 0;
	idx_t tc_successes = 0;
	idx_t tc_failures = 0;
	idx_t tc_loop1_iters = 0;
	idx_t tc_loop2_iters = 0;
	idx_t tc_runs_left = 0;
	idx_t tc_runs_right = 0;
	idx_t tc_hash_groups = 0;
	idx_t tc_loop2_entered = 0;

	void Print() {
		const auto total = no_arena + backoff_skips + trybuild_attempts + trycluster2_calls;
		if (!total) {
			return;
		}
		std::fprintf(stderr,
		             "[clustered trybuild] no_arena=%llu backoff_skips=%llu attempts=%llu successes=%llu "
		             "failures=%llu try2_calls=%llu try2_successes=%llu try2_fail_group_limit=%llu "
		             "try2_fail_collision=%llu rows_success=%llu rows_fail=%llu\n",
		             (unsigned long long)no_arena, (unsigned long long)backoff_skips,
		             (unsigned long long)trybuild_attempts, (unsigned long long)trybuild_successes,
		             (unsigned long long)trybuild_failures, (unsigned long long)trycluster2_calls,
		             (unsigned long long)trycluster2_successes, (unsigned long long)trycluster2_fail_group_limit,
		             (unsigned long long)trycluster2_fail_collision, (unsigned long long)trycluster2_rows_success,
		             (unsigned long long)trycluster2_rows_fail);
		if (tc_calls) {
			std::fprintf(stderr,
			             "[clustered TryClustered] calls=%llu ok=%llu fail=%llu "
			             "loop1_iters=%llu loop2_entered=%llu loop2_iters=%llu "
			             "hash_groups=%llu runs_left=%llu runs_right=%llu\n",
			             (unsigned long long)tc_calls, (unsigned long long)tc_successes,
			             (unsigned long long)tc_failures,
			             (unsigned long long)tc_loop1_iters, (unsigned long long)tc_loop2_entered,
			             (unsigned long long)tc_loop2_iters,
			             (unsigned long long)tc_hash_groups, (unsigned long long)tc_runs_left,
			             (unsigned long long)tc_runs_right);
		}
	}
	~ClusteredTryBuildDebugStats() { Print(); }
};

ClusteredTryBuildDebugStats clustered_trybuild_debug;
} // namespace

namespace duckdb {

bool ClusteredAggr::TryClustered2(const uint64_t *group_ids, idx_t count, sel_t *arena, uint32_t *slots) {
	// Partition tuple indices 0..count-1 into contiguous, ascending runs per group.
	//
	// Each group gets an arena slot with two cursors starting at the center:
	//   left grows downward ← writes indices from the first half (in reverse)
	//   right grows upward  → writes indices from the second half (forward)
	//
	// We scan the input from both ends toward the middle simultaneously, so the two
	// cursor streams are independent and can execute with higher IPC. After the scan,
	// left..right is a single contiguous, ascending sequence
	// because the left half was visited in descending index order and pushed downward.
	constexpr idx_t BUCKET_CAP = STANDARD_VECTOR_SIZE;
	constexpr idx_t CENTER_OFFSET = STANDARD_VECTOR_SIZE / 2;

	idx_t n_seen = 0;
	const idx_t group_limit = MinValue<idx_t>(MAX_GROUPS, count >> 3);
	enum class FailReason : uint8_t { NONE, GROUP_LIMIT, COLLISION };
	FailReason fail_reason = FailReason::NONE;
	clustered_trybuild_debug.trycluster2_calls++;
	sel_t *next_slot = arena;
	sel_t *left_cursor[MAX_GROUPS];
	sel_t *right_cursor[MAX_GROUPS];
	auto allocate_slot = [&](idx_t slot_idx, uint64_t gid) -> GroupRun * {
		if (n_seen >= group_limit) {
			if (fail_reason == FailReason::NONE) {
				fail_reason = FailReason::GROUP_LIMIT;
			}
			return nullptr;
		}
		sel_t *center = next_slot + CENTER_OFFSET;
		left_cursor[n_seen] = center;
		right_cursor[n_seen] = center;
		group_runs[n_seen].gid = gid;
		slots[slot_idx] = static_cast<uint32_t>(gid) | (static_cast<uint32_t>(n_seen) << POSITION_SHIFT);
		next_slot += BUCKET_CAP;
		n_seen++;
		return &group_runs[n_seen - 1];
	};
	auto find_or_allocate_slot = [&](uint64_t gid) -> GroupRun * {
		D_ASSERT(gid < MAX_GID_COUNT - 1);
		auto pos = gid & (HASH_SLOTS - 1);
		auto &slot = slots[pos];
		if (DUCKDB_LIKELY((slot & GID_MASK) == gid)) {
			return &group_runs[slot >> POSITION_SHIFT];
		} else if (DUCKDB_LIKELY(slot == INVALID_SLOT)) {
			return allocate_slot(pos, gid);
		} else {
			pos = (HASH_SLOTS - 1) - pos;
			auto &alt = slots[pos];
			if (DUCKDB_LIKELY((alt & GID_MASK) == gid)) {
				return &group_runs[alt >> POSITION_SHIFT];
			} else if (DUCKDB_LIKELY(alt == INVALID_SLOT)) {
				return allocate_slot(pos, gid);
			}
			if (fail_reason == FailReason::NONE) {
				fail_reason = FailReason::COLLISION;
			}
			return nullptr;
		}
	};
	auto finish = [&](bool result) {
		for (idx_t i = 0; i < n_seen; i++) {
			auto pos = group_runs[i].gid & (HASH_SLOTS - 1);
			slots[pos] = INVALID_SLOT;
			slots[(HASH_SLOTS - 1) - pos] = INVALID_SLOT;
		}
		n_group_runs = result ? n_seen : 0;
		if (result) {
			clustered_trybuild_debug.trycluster2_successes++;
			clustered_trybuild_debug.trycluster2_rows_success += count;
			cached_dict_sel = nullptr;
		} else {
			clustered_trybuild_debug.trycluster2_rows_fail += count;
			if (fail_reason == FailReason::GROUP_LIMIT) {
				clustered_trybuild_debug.trycluster2_fail_group_limit++;
			} else if (fail_reason == FailReason::COLLISION) {
				clustered_trybuild_debug.trycluster2_fail_collision++;
			}
		}
		return result;
	};

	// Scan from both ends toward the middle.
	const idx_t half = count / 2;
	for (idx_t i = 0; i < half; i++) {
		const idx_t j_left = half - 1 - i; // scans 0..half-1 in reverse
		const idx_t j_right = half + i;    // scans half..count-1 forward
		auto *group_left = find_or_allocate_slot(group_ids[j_left]);
		auto *group_right = find_or_allocate_slot(group_ids[j_right]);
		if (!group_left || !group_right) {
			return finish(false);
		}
		auto left_pos = static_cast<idx_t>(group_left - group_runs);
		auto right_pos = static_cast<idx_t>(group_right - group_runs);
		left_cursor[left_pos]--;
		left_cursor[left_pos][0] = static_cast<sel_t>(j_left);
		right_cursor[right_pos][0] = static_cast<sel_t>(j_right);
		right_cursor[right_pos]++;
	}
	for (idx_t j = 2 * half; j < count; j++) {
		auto *group = find_or_allocate_slot(group_ids[j]);
		if (!group) {
			return finish(false);
		}
		auto pos = static_cast<idx_t>(group - group_runs);
		right_cursor[pos][0] = static_cast<sel_t>(j);
		right_cursor[pos]++;
	}

	// Publish each group's contiguous run and reset the cursor tables for reuse.
	for (idx_t i = 0; i < n_seen; i++) {
		const sel_t *run_begin = left_cursor[i];
		const sel_t *run_end = right_cursor[i];
		const idx_t run_len = static_cast<idx_t>(run_end - run_begin);
		group_runs[i].sel = run_begin;
		group_runs[i].count = run_len;
	}
	return finish(true);
}

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
	sel_t *next_list = arena + STANDARD_VECTOR_SIZE;
	sel_t *left_list_cursor[MAX_RUNS];
	sel_t *right_list_cursor[MAX_RUNS];
	idx_t left_run_cursor = FIRST_GROUP;
	idx_t right_run_cursor = FIRST_GROUP;
	idx_t cur_num_groups = 0;
	idx_t local_loop1_iters = 0;

	clustered_trybuild_debug.tc_calls++;
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
		if (cur_num_groups == MAX_GROUPS || slots[slot] != INVALID_SLOT) {
    			right_run_cursor = FIRST_GROUP + cur_num_groups;
			return nullptr;
		}
		idx_t group_pos = FIRST_GROUP + cur_num_groups++; // early increment: 1st vector is for sequential runs (non-hashed)
		group_runs[group_pos].gid = gid;
		sel_t *list_center = (next_list += STANDARD_VECTOR_SIZE) + STANDARD_VECTOR_SIZE / 2;
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

#define LEFT_LIST_END(cur) ((cur + 1 == FIRST_GROUP) ? (arena + STANDARD_VECTOR_SIZE) : left_list_cursor[cur + 1])
#define RIGHT_LIST_START(cur) ((cur == FIRST_GROUP + cur_num_groups) ? arena : right_list_cursor[cur - 1])

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
		const auto gid_left = group_ids[cur_left];
		const auto gid_right = group_ids[cur_right];
		auto *gl = find_group(gid_left, slot_left);
		if (DUCKDB_UNLIKELY(!gl) && !(gl = append_group(gid_left, slot_left))) {
			base = i; break;
		}
		auto *gr = find_group(gid_right, slot_right);
		if (DUCKDB_UNLIKELY(!gr) && !(gr = append_group(gid_right, slot_right))) {
			base = i; break;
		}
		local_loop1_iters++;
		auto left_group = static_cast<idx_t>(gl - group_runs);
		auto right_group = static_cast<idx_t>(gr - group_runs);
		left_list_cursor[left_group]--;
		left_list_cursor[left_group][0] = static_cast<sel_t>(cur_left);
		right_list_cursor[right_group][0] = static_cast<sel_t>(cur_right);
		right_list_cursor[right_group]++;

	}
	clustered_trybuild_debug.tc_loop1_iters += local_loop1_iters;
	// continue, now appending only "runs" by looking at the previous gid (&detecting earlier "groups" in the hash table)
	right_run_cursor = FIRST_GROUP + cur_num_groups;
	if (base < half) {
		clustered_trybuild_debug.tc_loop2_entered++;
	}
	uint64_t prev_left_gid = ~uint64_t(0);
	uint64_t prev_right_gid = ~uint64_t(0);
	GroupRun *prev_left_group = nullptr;
	GroupRun *prev_right_group = nullptr;
        for(idx_t i = base; i < half; i++) {
		const idx_t cur_left = half - 1 - i;
		const idx_t cur_right = half + i;
		const auto gid_left = group_ids[cur_left];
		const auto gid_right = group_ids[cur_right];
		auto *gl = find_group_or_run(gid_left, slot_left, prev_left_gid, prev_left_group);
		auto *gr = find_group_or_run(gid_right, slot_right, prev_right_gid, prev_right_group);
                if (DUCKDB_UNLIKELY(!gl || !gr)) { 
			if (!gl) {
				gl = append_run_left(gid_left, slot_left);
			}
                	if (!gr) {
				gr = append_run_right(gid_right, slot_right);
			}
		}
		auto left_group = static_cast<idx_t>(gl - group_runs);
		auto right_group = static_cast<idx_t>(gr - group_runs);
		left_list_cursor[left_group]--;
		left_list_cursor[left_group][0] = static_cast<sel_t>(cur_left);
		right_list_cursor[right_group][0] = static_cast<sel_t>(cur_right);
		right_list_cursor[right_group]++;

		clustered_trybuild_debug.tc_loop2_iters++;
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

        // postprocessing: set the .count and start pointer (.sel) of the RunGroups in-place
	for(auto cur = left_run_cursor; cur < FIRST_GROUP; cur++) { // Left Runs
		group_runs[cur].sel = left_list_cursor[cur];
		group_runs[cur].count = static_cast<idx_t>(LEFT_LIST_END(cur) - left_list_cursor[cur]);
	}
	for(auto cur = FIRST_GROUP; cur < FIRST_GROUP + cur_num_groups; cur++) { // exact/hashed Groups
		group_runs[cur].sel = left_list_cursor[cur];
		group_runs[cur].count = static_cast<idx_t>(right_list_cursor[cur] - left_list_cursor[cur]);
	}
	for(auto cur = FIRST_GROUP + cur_num_groups; cur < right_run_cursor; cur++) { // Right Runs
		group_runs[cur].sel = RIGHT_LIST_START(cur);
		group_runs[cur].count = static_cast<idx_t>(right_list_cursor[cur] - RIGHT_LIST_START(cur));
	}
	idx_t dst = right_run_cursor - left_run_cursor;
	idx_t tuples_in_large_runs = 0;
	for (auto cur = left_run_cursor; cur < right_run_cursor; cur++) {
		if (group_runs[cur].count >= MAJORITY_RUNLENGTH_THRESHOLD) {
			tuples_in_large_runs += group_runs[cur].count;
		}
	}
	if (2 * tuples_in_large_runs < count || dst > STANDARD_VECTOR_SIZE / 2 + 1) {
		n_group_runs = 0;
		clustered_trybuild_debug.tc_failures++;
		return cleanup(false);
	}
	run_begin = left_run_cursor;
	n_group_runs = dst;

	clustered_trybuild_debug.tc_successes++;
	clustered_trybuild_debug.tc_hash_groups += cur_num_groups;
	clustered_trybuild_debug.tc_runs_left += (FIRST_GROUP - left_run_cursor);
	clustered_trybuild_debug.tc_runs_right += (right_run_cursor - FIRST_GROUP - cur_num_groups);
	return cleanup(true); 
}

void ClusteredAggr::SetSingleRun(data_ptr_t state, idx_t count) {
	run_begin = 0;
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
		clustered_trybuild_debug.no_arena++;
		return false;
	}
	if (skipped_opportunities > 0) {
		clustered_trybuild_debug.backoff_skips++;
		skipped_opportunities--;
		return false;
	}
	clustered_trybuild_debug.trybuild_attempts++;
	if (clustered.TryClustered(group_ids, count, arena.get(), slots.get())) {
		clustered_trybuild_debug.trybuild_successes++;
		skipped_opportunities = 0;
		retry_backoff = 1;
		return true;
	}
	clustered_trybuild_debug.trybuild_failures++;
	skipped_opportunities = retry_backoff;
	retry_backoff = MinValue<idx_t>(NumericLimits<idx_t>::Maximum() / 2, retry_backoff) * 2;
	auto total = clustered_trybuild_debug.trybuild_attempts;
	if (total > 0 && (total & (total - 1)) == 0) {
		clustered_trybuild_debug.Print();
	}
	return false;
}
} // namespace duckdb
