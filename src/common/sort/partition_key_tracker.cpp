#include "duckdb/common/sorting/partition_key_tracker.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

static constexpr idx_t PARTITION_KEY_TRACKER_CAPACITY =
    RadixPartitioning::NumberOfPartitions(RadixPartitioning::MAX_RADIX_BITS);

template <bool USE_PARTITION_SEL>
static idx_t GetPartitionRowIndex(PartitionedTupleDataAppendState &append_state, idx_t idx) {
	if constexpr (USE_PARTITION_SEL) {
		return append_state.partition_sel.get_index_unsafe(idx);
	}
	return idx;
}

PartitionKeyTracker::PartitionKeyTracker(Allocator &allocator_p, const vector<LogicalType> &key_types_p)
    : key_count(key_types_p.size()) {
	single_value_sel.Initialize();
	candidate_input_sel.Initialize(PARTITION_KEY_TRACKER_CAPACITY);
	candidate_rep_sel.Initialize(PARTITION_KEY_TRACKER_CAPACITY);
	mismatch_sel.Initialize(PARTITION_KEY_TRACKER_CAPACITY);
	representatives.Initialize(allocator_p, key_types_p, PARTITION_KEY_TRACKER_CAPACITY);
}

void PartitionKeyTracker::Reset(idx_t radix_bits_p) {
	radix_bits = radix_bits_p;
	const auto partition_count = RadixPartitioning::NumberOfPartitions(radix_bits);
	D_ASSERT(partition_count <= PARTITION_KEY_TRACKER_CAPACITY);
	states.clear();
	states.resize(partition_count, PartitionKeyTrackerState::EMPTY);
	hashes.clear();
	hashes.resize(partition_count);
	representatives.Reset();
	representatives.SetChildCardinality(partition_count);
}

bool PartitionKeyTracker::CanBypass(idx_t hash_bin) const {
	return hash_bin < states.size() && states[hash_bin] == PartitionKeyTrackerState::SINGLE_KEY;
}

void PartitionKeyTracker::Update(DataChunk &keys, Vector &input_hashes, PartitionedTupleDataAppendState &append_state,
                                 idx_t count) {
	if (states.empty() || !count) {
		return;
	}

	D_ASSERT(keys.ColumnCount() == key_count);
	D_ASSERT(count <= STANDARD_VECTOR_SIZE);

	idx_t candidate_count;
	if (append_state.fixed_partition_entries.size()) {
		if (append_state.fixed_partition_entries.size() > 1) {
			candidate_count = BuildCandidates<true, true>(keys, input_hashes, append_state, count);
		} else {
			candidate_count = BuildCandidates<true, false>(keys, input_hashes, append_state, count);
		}
	} else {
		if (append_state.partition_entries.size() > 1) {
			candidate_count = BuildCandidates<false, true>(keys, input_hashes, append_state, count);
		} else {
			candidate_count = BuildCandidates<false, false>(keys, input_hashes, append_state, count);
		}
	}
	if (candidate_count) {
		CompareCandidates(keys, candidate_count);
	}
}

void PartitionKeyTracker::Combine(const PartitionKeyTracker &other) {
	if (other.states.empty()) {
		return;
	}
	if (states.empty()) {
		Reset(other.radix_bits);
	}
	D_ASSERT(radix_bits == other.radix_bits);
	D_ASSERT(states.size() == other.states.size());
	D_ASSERT(states.size() <= PARTITION_KEY_TRACKER_CAPACITY);

	idx_t candidate_count = 0;
	for (idx_t bin_idx = 0; bin_idx < states.size(); bin_idx++) {
		CombineBin(other, bin_idx, candidate_count);
	}
	if (candidate_count) {
		CompareTrackerCandidates(other, candidate_count);
	}
}

void PartitionKeyTracker::StoreRepresentative(DataChunk &keys, idx_t row_idx, hash_t hash, idx_t bin_idx) {
	states[bin_idx] = PartitionKeyTrackerState::SINGLE_KEY;
	this->hashes[bin_idx] = hash;
	single_value_sel.set_index(0, row_idx);
	for (idx_t col_idx = 0; col_idx < key_count; col_idx++) {
		representatives.data[col_idx].Copy(keys.data[col_idx], single_value_sel, 1, 0, bin_idx, 1);
	}
}

void PartitionKeyTracker::StoreRepresentative(const PartitionKeyTracker &source, idx_t source_bin, idx_t target_bin) {
	states[target_bin] = PartitionKeyTrackerState::SINGLE_KEY;
	hashes[target_bin] = source.hashes[source_bin];
	single_value_sel.set_index(0, source_bin);
	for (idx_t col_idx = 0; col_idx < key_count; col_idx++) {
		representatives.data[col_idx].Copy(source.representatives.data[col_idx], single_value_sel, 1, 0, target_bin, 1);
	}
}

void PartitionKeyTracker::MarkMixed(idx_t bin_idx) {
	states[bin_idx] = PartitionKeyTrackerState::MULTIPLE_KEYS;
}

template <bool FIXED, bool USE_PARTITION_SEL>
idx_t PartitionKeyTracker::BuildCandidates(DataChunk &keys, Vector &input_hashes,
                                           PartitionedTupleDataAppendState &append_state, const idx_t count) {
	using GETTER = TemplatedMapGetter<list_entry_t, FIXED>;
	auto &partition_entries = append_state.GetMap<FIXED>();
	UnifiedVectorFormat hash_data;
	input_hashes.ToUnifiedFormat(hash_data);
	const auto hash_values = UnifiedVectorFormat::GetData<hash_t>(hash_data);
	idx_t candidate_count = 0;
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		const auto bin_idx = GETTER::GetKey(it);
		const auto &entry = GETTER::GetValue(it);

		if (states[bin_idx] == PartitionKeyTrackerState::MULTIPLE_KEYS) {
			continue;
		}

		idx_t entry_idx = 0;
		if (states[bin_idx] == PartitionKeyTrackerState::EMPTY) {
			const auto row_idx = GetPartitionRowIndex<USE_PARTITION_SEL>(append_state, entry.offset);
			D_ASSERT(row_idx < count);
			const auto hash_idx = hash_data.sel->get_index(row_idx);
			StoreRepresentative(keys, row_idx, hash_values[hash_idx], bin_idx);
			entry_idx++;
		}

		const auto candidate_start = candidate_count;
		bool hash_mismatch = false;
		for (; entry_idx < entry.length; entry_idx++) {
			const auto row_idx = GetPartitionRowIndex<USE_PARTITION_SEL>(append_state, entry.offset + entry_idx);
			D_ASSERT(row_idx < count);
			const auto hash_idx = hash_data.sel->get_index(row_idx);
			const auto hash_match = this->hashes[bin_idx] == hash_values[hash_idx];
			hash_mismatch |= !hash_match;
			candidate_input_sel.set_index(candidate_count, row_idx);
			candidate_rep_sel.set_index(candidate_count, bin_idx);
			candidate_count += hash_match;
		}
		states[bin_idx] = hash_mismatch ? PartitionKeyTrackerState::MULTIPLE_KEYS : states[bin_idx];
		candidate_count = hash_mismatch ? candidate_start : candidate_count;
	}
	return candidate_count;
}

idx_t PartitionKeyTracker::CompactCandidates(idx_t candidate_count) {
	idx_t new_count = 0;
	for (idx_t candidate_idx = 0; candidate_idx < candidate_count; candidate_idx++) {
		const auto bin_idx = candidate_rep_sel.get_index_unsafe(candidate_idx);
		const auto input_idx = candidate_input_sel.get_index_unsafe(candidate_idx);
		const auto keep = states[bin_idx] == PartitionKeyTrackerState::SINGLE_KEY;
		candidate_input_sel.set_index(new_count, input_idx);
		candidate_rep_sel.set_index(new_count, bin_idx);
		new_count += keep;
	}
	return new_count;
}

void PartitionKeyTracker::CompareCandidates(DataChunk &keys, idx_t candidate_count) {
	D_ASSERT(candidate_count <= STANDARD_VECTOR_SIZE);
	for (idx_t col_idx = 0; col_idx < key_count && candidate_count; col_idx++) {
		Vector input_slice(keys.data[col_idx], candidate_input_sel, candidate_count);
		Vector representative_slice(representatives.data[col_idx], candidate_rep_sel, candidate_count);
		const auto mismatch_count = VectorOperations::DistinctFrom(input_slice, representative_slice, nullptr,
		                                                           candidate_count, &mismatch_sel, nullptr);
		for (idx_t mismatch_idx = 0; mismatch_idx < mismatch_count; mismatch_idx++) {
			const auto candidate_idx = mismatch_sel.get_index_unsafe(mismatch_idx);
			MarkMixed(candidate_rep_sel.get_index_unsafe(candidate_idx));
		}
		if (mismatch_count && col_idx + 1 < key_count) {
			candidate_count = CompactCandidates(candidate_count);
		}
	}
}

void PartitionKeyTracker::CombineBin(const PartitionKeyTracker &source, idx_t bin_idx, idx_t &candidate_count) {
	if (source.states[bin_idx] == PartitionKeyTrackerState::EMPTY ||
	    states[bin_idx] == PartitionKeyTrackerState::MULTIPLE_KEYS) {
		return;
	}
	if (source.states[bin_idx] == PartitionKeyTrackerState::MULTIPLE_KEYS) {
		MarkMixed(bin_idx);
		return;
	}
	D_ASSERT(source.states[bin_idx] == PartitionKeyTrackerState::SINGLE_KEY);
	if (states[bin_idx] == PartitionKeyTrackerState::EMPTY) {
		StoreRepresentative(source, bin_idx, bin_idx);
		return;
	}
	D_ASSERT(states[bin_idx] == PartitionKeyTrackerState::SINGLE_KEY);
	if (hashes[bin_idx] != source.hashes[bin_idx]) {
		MarkMixed(bin_idx);
		return;
	}
	candidate_input_sel.set_index(candidate_count, bin_idx);
	candidate_rep_sel.set_index(candidate_count, bin_idx);
	candidate_count++;
}

idx_t PartitionKeyTracker::CompactTrackerCandidates(idx_t candidate_count) {
	idx_t new_count = 0;
	for (idx_t candidate_idx = 0; candidate_idx < candidate_count; candidate_idx++) {
		const auto bin_idx = candidate_input_sel.get_index_unsafe(candidate_idx);
		const auto source_bin_idx = candidate_rep_sel.get_index_unsafe(candidate_idx);
		const auto keep = states[bin_idx] == PartitionKeyTrackerState::SINGLE_KEY;
		candidate_input_sel.set_index(new_count, bin_idx);
		candidate_rep_sel.set_index(new_count, source_bin_idx);
		new_count += keep;
	}
	return new_count;
}

void PartitionKeyTracker::CompareTrackerCandidates(const PartitionKeyTracker &source, idx_t candidate_count) {
	D_ASSERT(candidate_count <= PARTITION_KEY_TRACKER_CAPACITY);
	for (idx_t col_idx = 0; col_idx < key_count && candidate_count; col_idx++) {
		Vector target_slice(representatives.data[col_idx], candidate_input_sel, candidate_count);
		Vector source_slice(source.representatives.data[col_idx], candidate_rep_sel, candidate_count);
		const auto mismatch_count = VectorOperations::DistinctFrom(target_slice, source_slice, nullptr, candidate_count,
		                                                           &mismatch_sel, nullptr);
		for (idx_t mismatch_idx = 0; mismatch_idx < mismatch_count; mismatch_idx++) {
			const auto candidate_idx = mismatch_sel.get_index_unsafe(mismatch_idx);
			MarkMixed(candidate_input_sel.get_index_unsafe(candidate_idx));
		}
		if (mismatch_count && col_idx + 1 < key_count) {
			candidate_count = CompactTrackerCandidates(candidate_count);
		}
	}
}

template idx_t PartitionKeyTracker::BuildCandidates<true, true>(DataChunk &, Vector &,
                                                                PartitionedTupleDataAppendState &, idx_t);
template idx_t PartitionKeyTracker::BuildCandidates<true, false>(DataChunk &, Vector &,
                                                                 PartitionedTupleDataAppendState &, idx_t);
template idx_t PartitionKeyTracker::BuildCandidates<false, true>(DataChunk &, Vector &,
                                                                 PartitionedTupleDataAppendState &, idx_t);
template idx_t PartitionKeyTracker::BuildCandidates<false, false>(DataChunk &, Vector &,
                                                                  PartitionedTupleDataAppendState &, idx_t);

RepartitionKeyTracker::RepartitionKeyTracker(Allocator &allocator, PartitionKeyTracker &tracker_p,
                                             const vector<LogicalType> &key_types,
                                             const vector<column_t> &partition_key_ids_p)
    : tracker(tracker_p), partition_key_ids(partition_key_ids_p) {
	keys.Initialize(allocator, key_types);
}

void RepartitionKeyTracker::RepartitionChunk(TupleDataCollection &source_partition, TupleDataChunkState &source_chunk,
                                             PartitionedTupleDataAppendState &target_append, idx_t count) {
	if (!count) {
		return;
	}

	if (key_gather_state.column_ids.empty()) {
		source_partition.InitializeChunkState(key_gather_state, partition_key_ids);
	}

	keys.Reset();
	TupleDataCollection::ResetCachedCastVectors(key_gather_state, partition_key_ids);
	source_partition.Gather(source_chunk.row_locations, *FlatVector::IncrementalSelectionVector(), count,
	                        partition_key_ids, keys, *FlatVector::IncrementalSelectionVector(),
	                        key_gather_state.cached_cast_vectors);
	keys.SetChildCardinality(count);

	D_ASSERT(target_append.utility_vector);
	tracker.Update(keys, *target_append.utility_vector, target_append, count);
}

} // namespace duckdb
