#include "duckdb/common/clustered_aggr.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

bool ClusteredAggr::TryClustered(const uint64_t *group_ids, idx_t count, sel_t *arena, uint32_t *slots) {
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

	idx_t n_seen = 0;
	const idx_t group_limit = MinValue<idx_t>(MAX_GROUPS, count >> 3);
	sel_t *next_slot = arena;
	sel_t *left_cursor[MAX_GROUPS];
	sel_t *right_cursor[MAX_GROUPS];
	auto allocate_slot = [&](idx_t slot_idx, uint64_t gid) -> GroupRun * {
		if (n_seen >= group_limit) {
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
			cached_dict_sel = nullptr;
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
