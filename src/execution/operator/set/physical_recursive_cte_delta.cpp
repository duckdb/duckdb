#include "duckdb/execution/operator/set/physical_recursive_cte_delta.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

RecursiveCTEKeyDeltaState::RecursiveCTEKeyDeltaState(ClientContext &context, const PhysicalRecursiveCTE &op,
                                                     bool track_touched_keys)
    : previous_rows(context, op.GetTypes()), new_keys(context, op.distinct_types),
      touched_addresses(LogicalType::POINTER), matched_addresses(LogicalType::POINTER),
      first_touches(STANDARD_VECTOR_SIZE), found_groups(STANDARD_VECTOR_SIZE), missing_groups(STANDARD_VECTOR_SIZE),
      changed_groups(STANDARD_VECTOR_SIZE), changed_column_groups(STANDARD_VECTOR_SIZE),
      equal_groups_a(STANDARD_VECTOR_SIZE), equal_groups_b(STANDARD_VECTOR_SIZE), arena(Allocator::Get(context)),
      row_state(arena) {
	first_touch_keys.Initialize(Allocator::Get(context), op.distinct_types);
	selected_keys.Initialize(Allocator::Get(context), op.distinct_types);
	payload_rows.Initialize(Allocator::Get(context), op.payload_types);
	result_rows.Initialize(Allocator::Get(context), op.GetTypes());
	changed_rows.Initialize(Allocator::Get(context), op.GetTypes());
	previous_scan_rows.Initialize(Allocator::Get(context), op.GetTypes());
	key_scan_rows.Initialize(Allocator::Get(context), op.distinct_types);
	if (track_touched_keys) {
		touched_keys = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types);
	}
}

void RecursiveCTEKeyDeltaState::Reset() {
	if (touched_keys) {
		touched_keys->ResetForNewIteration(0);
	}
	previous_rows.ResetForReuse();
	previous_rows.InitializeAppend(previous_append_state);
	new_keys.ResetForReuse();
	new_keys.InitializeAppend(new_key_append_state);
	touched_count = 0;
	new_count = 0;
	changed_count = 0;
}

static void GatherDeltaChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
	}
}

static void ScatterDeltaChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
	}
}

static idx_t SelectDistinctPayloadRows(const Vector &previous, const Vector &current,
                                       optional_ptr<const SelectionVector> candidates, idx_t candidate_count,
                                       SelectionVector &changed, SelectionVector &unchanged) {
	if (!candidates) {
		return VectorOperations::DistinctFrom(previous, current, nullptr, candidate_count, &changed, &unchanged);
	}
	Vector previous_candidates(previous, *candidates, candidate_count);
	Vector current_candidates(current, *candidates, candidate_count);
	return VectorOperations::DistinctFrom(previous_candidates, current_candidates, candidates, candidate_count,
	                                      &changed, &unchanged);
}

void RecursiveCTEState::SnapshotUsingKeyDelta(DataChunk &keys) {
	D_ASSERT(key_delta);
	auto &delta = *key_delta;
	D_ASSERT(delta.touched_keys);
	const auto first_touch_count =
	    delta.touched_keys->FindOrCreateGroups(keys, delta.touched_addresses, delta.first_touches);
	if (first_touch_count == 0) {
		return;
	}

	delta.first_touch_keys.Reset();
	delta.first_touch_keys.Slice(keys, delta.first_touches, first_touch_count);
	SnapshotUsingKeyDeltaGroups(delta.first_touch_keys);
}

void RecursiveCTEState::SnapshotUsingKeyDeltaGroups(DataChunk &keys) {
	D_ASSERT(key_delta);
	auto &delta = *key_delta;
	const auto first_touch_count = keys.size();
	delta.touched_count += first_touch_count;
	if (first_touch_count == 0) {
		return;
	}
	const auto found_count = ht->LookupGroups(keys, delta.lookup_state, delta.found_groups);

	idx_t found_idx = 0;
	idx_t missing_count = 0;
	for (idx_t key_idx = 0; key_idx < first_touch_count; key_idx++) {
		if (found_idx < found_count && delta.found_groups.get_index_unsafe(found_idx) == key_idx) {
			found_idx++;
			continue;
		}
		delta.missing_groups.set_index(missing_count++, key_idx);
	}
	D_ASSERT(found_idx == found_count);
	delta.new_count += missing_count;

	if (found_count > 0) {
		delta.selected_keys.Reset();
		delta.selected_keys.Slice(keys, delta.found_groups, found_count);

		delta.matched_addresses.SetVectorType(VectorType::FLAT_VECTOR);
		auto source_addresses = FlatVector::GetData<data_ptr_t>(delta.lookup_state.addresses);
		auto target_addresses = FlatVector::GetDataMutable<data_ptr_t>(delta.matched_addresses);
		for (idx_t match_idx = 0; match_idx < found_count; match_idx++) {
			target_addresses[match_idx] = source_addresses[delta.found_groups.get_index_unsafe(match_idx)];
		}
		FlatVector::SetSize(delta.matched_addresses, found_count);

		delta.payload_rows.Reset();
		delta.payload_rows.SetChildCardinality(found_count);
		if (delta.payload_rows.ColumnCount() > 0) {
			FinalizePayload(delta.row_state, delta.matched_addresses, delta.payload_rows, 0);
		}
		delta.result_rows.Reset();
		ScatterDeltaChunk(delta.result_rows, delta.selected_keys, op.distinct_idx);
		ScatterDeltaChunk(delta.result_rows, delta.payload_rows, op.payload_idx);
		delta.result_rows.CheckCardinality(found_count);
		delta.previous_rows.Append(delta.previous_append_state, delta.result_rows);
	}

	if (missing_count > 0) {
		delta.selected_keys.Reset();
		delta.selected_keys.Slice(keys, delta.missing_groups, missing_count);
		delta.new_keys.Append(delta.new_key_append_state, delta.selected_keys);
	}
}

idx_t RecursiveCTEState::FinalizeUsingKeyDelta(bool update_partial_indexes, bool collect_metrics) {
	D_ASSERT(key_delta);
	auto &delta = *key_delta;
	ColumnDataScanState previous_scan_state;
	delta.previous_rows.InitializeScan(previous_scan_state);
	while (delta.previous_rows.Scan(previous_scan_state, delta.previous_scan_rows)) {
		const auto row_count = delta.previous_scan_rows.size();
		delta.key_scan_rows.Reset();
		GatherDeltaChunk(delta.key_scan_rows, delta.previous_scan_rows, op.distinct_idx);
		const auto found_count = ht->LookupGroups(delta.key_scan_rows, delta.lookup_state, delta.found_groups);
		if (found_count != row_count) {
			throw InternalException("USING KEY delta finalization could not find %d of %d touched groups",
			                        row_count - found_count, row_count);
		}

		delta.payload_rows.Reset();
		delta.payload_rows.SetChildCardinality(row_count);
		if (delta.payload_rows.ColumnCount() > 0) {
			FinalizePayload(delta.row_state, delta.lookup_state.addresses, delta.payload_rows, 0);
		}
		delta.result_rows.Reset();
		ScatterDeltaChunk(delta.result_rows, delta.key_scan_rows, op.distinct_idx);
		ScatterDeltaChunk(delta.result_rows, delta.payload_rows, op.payload_idx);
		delta.result_rows.CheckCardinality(row_count);

		idx_t changed_count = 0;
		idx_t equal_count = row_count;
		optional_ptr<const SelectionVector> equal_groups;
		for (idx_t payload_idx = 0; payload_idx < op.payload_idx.size() && equal_count > 0; payload_idx++) {
			auto &next_equal_groups = payload_idx % 2 == 0 ? delta.equal_groups_a : delta.equal_groups_b;
			const auto changed_column_count = SelectDistinctPayloadRows(
			    delta.previous_scan_rows.data[op.payload_idx[payload_idx]], delta.payload_rows.data[payload_idx],
			    equal_groups, equal_count, delta.changed_column_groups, next_equal_groups);
			for (idx_t changed_idx = 0; changed_idx < changed_column_count; changed_idx++) {
				delta.changed_groups.set_index(changed_count++,
				                               delta.changed_column_groups.get_index_unsafe(changed_idx));
			}
			equal_count -= changed_column_count;
			equal_groups = &next_equal_groups;
		}
		if (changed_count > 0) {
			delta.changed_count += changed_count;
			delta.changed_rows.Reset();
			delta.changed_rows.Slice(delta.result_rows, delta.changed_groups, changed_count);
			op.working_table->Append(working_append_state, delta.changed_rows);
		}
	}

	ColumnDataScanState new_key_scan_state;
	idx_t index_work_ns = 0;
	delta.new_keys.InitializeScan(new_key_scan_state);
	while (delta.new_keys.Scan(new_key_scan_state, delta.key_scan_rows)) {
		const auto row_count = delta.key_scan_rows.size();
		const auto found_count = ht->LookupGroups(delta.key_scan_rows, delta.lookup_state, delta.found_groups);
		if (found_count != row_count) {
			throw InternalException("USING KEY delta finalization could not find %d of %d new groups",
			                        row_count - found_count, row_count);
		}
		if (update_partial_indexes) {
			const auto index_start =
			    collect_metrics ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			for (auto &index : partial_key_indexes) {
				index->AddGroups(delta.key_scan_rows, *FlatVector::IncrementalSelectionVector(),
				                 delta.lookup_state.addresses, row_count);
			}
			if (collect_metrics) {
				const auto index_end = std::chrono::steady_clock::now();
				const auto elapsed_ns = NumericCast<idx_t>(
				    std::chrono::duration_cast<std::chrono::nanoseconds>(index_end - index_start).count());
				index_work_ns += elapsed_ns;
				GetEpochMetrics().RecordPartialIndexMaintenance(elapsed_ns);
				metrics.RecordPartialIndexBuild(NumericCast<idx_t>(elapsed_ns / 1000));
			}
		}

		delta.payload_rows.Reset();
		delta.payload_rows.SetChildCardinality(row_count);
		if (delta.payload_rows.ColumnCount() > 0) {
			FinalizePayload(delta.row_state, delta.lookup_state.addresses, delta.payload_rows, 0);
		}
		delta.result_rows.Reset();
		ScatterDeltaChunk(delta.result_rows, delta.key_scan_rows, op.distinct_idx);
		ScatterDeltaChunk(delta.result_rows, delta.payload_rows, op.payload_idx);
		delta.result_rows.CheckCardinality(row_count);
		op.working_table->Append(working_append_state, delta.result_rows);
	}
	return index_work_ns;
}

} // namespace duckdb
