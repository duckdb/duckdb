#include "duckdb/common/clustered_aggr.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

bool ClusteredAggr::TryClustered(const uint64_t *group_ids, idx_t count, sel_t *arena, Slot *slots) {
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
	static_assert((HASH_SLOTS & (HASH_SLOTS - 1)) == 0, "ClusteredAggr::HASH_SLOTS must be a power of two");

	idx_t seen_slots[MAX_GROUPS];
	idx_t n_seen = 0;
	sel_t *next_slot = arena;
	sel_t *const arena_end = arena + MAX_GROUPS * BUCKET_CAP;

	auto get_slot = [&](uint64_t gid) -> Slot * {
		return &slots[gid & (HASH_SLOTS - 1)];
	};
	auto allocate_slot = [&](Slot &slot, uint64_t gid) {
		if (next_slot == arena_end) {
			return false;
		}
		sel_t *center = next_slot + CENTER_OFFSET;
		slot.left = center;
		slot.right = center;
		slot.gid = gid;
		next_slot += BUCKET_CAP;
		seen_slots[n_seen++] = gid & (HASH_SLOTS - 1);
		return true;
	};
	auto bail_out = [&]() {
		for (idx_t i = 0; i < n_seen; i++) {
			auto &slot = slots[seen_slots[i]];
			slot.left = nullptr;
			slot.right = nullptr;
		}
		n_group_runs = 0;
		return false;
	};

	// Scan from both ends toward the middle.
	const idx_t half = count / 2;
	for (idx_t i = 0; i < half; i++) {
		const idx_t j_left = half - 1 - i; // scans 0..half-1 in reverse
		const idx_t j_right = half + i;    // scans half..count-1 forward
		const auto gid_left = group_ids[j_left];
		const auto gid_right = group_ids[j_right];
		auto &slot_left = *get_slot(gid_left);
		auto &slot_right = *get_slot(gid_right);
		if (slot_left.left == nullptr) {
			if (!allocate_slot(slot_left, gid_left)) {
				return bail_out();
			}
		} else if (slot_left.gid != gid_left) {
			return bail_out();
		}
		if (slot_right.right == nullptr) {
			if (!allocate_slot(slot_right, gid_right)) {
				return bail_out();
			}
		} else if (slot_right.gid != gid_right) {
			return bail_out();
		}
		// The two cursor updates are independent (different groups or different
		// directions within the same slot), giving the CPU two parallel store chains.
		slot_left.left--;
		slot_left.left[0] = static_cast<sel_t>(j_left);
		slot_right.right[0] = static_cast<sel_t>(j_right);
		slot_right.right++;
	}
	// Handle the odd element if count is odd.
	for (idx_t j = 2 * half; j < count; j++) {
		const auto gid = group_ids[j];
		auto &slot = *get_slot(gid);
		if (slot.right == nullptr) {
			if (!allocate_slot(slot, gid)) {
				return bail_out();
			}
		} else if (slot.gid != gid) {
			return bail_out();
		}
		slot.right[0] = static_cast<sel_t>(j);
		slot.right++;
	}

	// Publish each group's contiguous run and reset the cursor tables for reuse.
	for (idx_t i = 0; i < n_seen; i++) {
		auto &slot = slots[seen_slots[i]];
		const sel_t *run_begin = slot.left;
		const sel_t *run_end = slot.right;
		const idx_t run_len = static_cast<idx_t>(run_end - run_begin);
		group_runs[i].sel = run_begin;
		group_runs[i].count = run_len;
		group_runs[i].gid = slot.gid;
		slot.left = nullptr;
		slot.right = nullptr;
	}
	n_group_runs = n_seen;
	cached_dict_sel = nullptr;
	return true;
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
		group_runs[r].state += payload_size;
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
		const auto *run_sel = group_runs[r].sel;
		const auto run_count = group_runs[r].count;
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
	arena = make_unsafe_uniq_array_uninitialized<sel_t>(ClusteredAggr::MAX_GROUPS * STANDARD_VECTOR_SIZE);
	slots = make_unsafe_uniq_array<ClusteredAggr::Slot>(ClusteredAggr::HASH_SLOTS);
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
