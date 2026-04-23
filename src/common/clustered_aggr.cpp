#include "duckdb/common/clustered_aggr.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

bool ClusteredAggr::TryClustered(const uint64_t *group_ids, idx_t count, uint16_t *arena, uint16_t **left_cursor,
                                 uint16_t **right_cursor) {
	// Each group gets one arena slot. The left cursor writes the first half backwards,
	// the right cursor writes the second half forwards, so every group's final range is
	// contiguous and still in input order.
	constexpr idx_t BUCKET_CAP = STANDARD_VECTOR_SIZE;
	constexpr idx_t CENTER_OFFSET = STANDARD_VECTOR_SIZE / 2;

	sel_t seen_group_ids[MAX_GROUPS];
	idx_t n_seen = 0;
	uint16_t *next_slot = arena;
	uint16_t *const arena_end = arena + MAX_GROUPS * BUCKET_CAP;

	auto allocate_slot = [&](uint64_t gid) {
		if (next_slot == arena_end) {
			return false;
		}
		uint16_t *center = next_slot + CENTER_OFFSET;
		left_cursor[gid] = center;
		right_cursor[gid] = center;
		next_slot += BUCKET_CAP;
		seen_group_ids[n_seen++] = static_cast<sel_t>(gid);
		return true;
	};
	auto bail_out = [&]() {
		for (idx_t i = 0; i < n_seen; i++) {
			left_cursor[seen_group_ids[i]] = nullptr;
			right_cursor[seen_group_ids[i]] = nullptr;
		}
		n_group_runs = 0;
		return false;
	};

	const idx_t half = count / 2;
	for (idx_t i = 0; i < half; i++) {
		const idx_t j_left = half - 1 - i;
		const idx_t j_right = half + i;
		const auto gid_left = group_ids[j_left];
		const auto gid_right = group_ids[j_right];
		if (left_cursor[gid_left] == nullptr) {
			if (!allocate_slot(gid_left)) {
				return bail_out();
			}
		}
		if (right_cursor[gid_right] == nullptr) {
			if (!allocate_slot(gid_right)) {
				return bail_out();
			}
		}
		*(--left_cursor[gid_left]) = static_cast<uint16_t>(j_left);
		*(right_cursor[gid_right]++) = static_cast<uint16_t>(j_right);
	}
	for (idx_t j = 2 * half; j < count; j++) {
		const auto gid = group_ids[j];
		if (right_cursor[gid] == nullptr) {
			if (!allocate_slot(gid)) {
				return bail_out();
			}
		}
		*(right_cursor[gid]++) = static_cast<uint16_t>(j);
	}

	// Materialize run order into sel[] and reset the cursor tables for reuse.
	idx_t out_pos = 0;
	for (idx_t i = 0; i < n_seen; i++) {
		const sel_t gid = seen_group_ids[i];
		const uint16_t *run_begin = left_cursor[gid];
		const uint16_t *run_end = right_cursor[gid];
		const idx_t run_len = static_cast<idx_t>(run_end - run_begin);
		for (idx_t k = 0; k < run_len; k++) {
			sel[out_pos + k] = static_cast<sel_t>(run_begin[k]);
		}
		group_runs[i].count = run_len;
		group_id_per_run[i] = static_cast<uint16_t>(gid);
		left_cursor[gid] = nullptr;
		right_cursor[gid] = nullptr;
		out_pos += run_len;
	}
	n_group_runs = n_seen;
	cached_dict_sel = nullptr;
	return true;
}

void ClusteredAggr::AdvanceStates(idx_t payload_size) {
	for (idx_t r = 0; r < n_group_runs; r++) {
		group_runs[r].state += payload_size;
	}
}

const sel_t *ClusteredAggr::ClusterIter(const Vector &input, idx_t count) const {
	switch (input.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		return sel;
	case VectorType::DICTIONARY_VECTOR: {
		auto &child = DictionaryVector::Child(input);
		if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
			return nullptr;
		}
		auto &dict_sel = DictionaryVector::SelVector(input);
		const sel_t *dict_data = dict_sel.data();
		if (dict_data == nullptr) {
			return sel;
		}
		if (cached_dict_sel == dict_data) {
			return composed_sel_data;
		}
		for (idx_t k = 0; k < count; k++) {
			composed_sel_data[k] = dict_data[sel[k]];
		}
		cached_dict_sel = dict_data;
		return composed_sel_data;
	}
	default:
		return nullptr;
	}
}

} // namespace duckdb
