#include "duckdb/execution/operator/join/outer_join_marker.hpp"

namespace duckdb {

OuterJoinMarker::OuterJoinMarker(bool enabled_p) : enabled(enabled_p), count(0) {
}

void OuterJoinMarker::Initialize(idx_t count_p) {
	if (!enabled) {
		return;
	}
	this->count = count_p;
	found_match = unique_ptr<bool[]>(new bool[count]);
	Reset();
}

void OuterJoinMarker::Reset() {
	if (!enabled) {
		return;
	}
	memset(found_match.get(), 0, sizeof(bool) * count);
}

void OuterJoinMarker::SetMatch(idx_t position) {
	if (!enabled) {
		return;
	}
	D_ASSERT(position < count);
	found_match[position] = true;
}

void OuterJoinMarker::SetMatches(const SelectionVector &sel, idx_t count, idx_t base_idx) {
	if (!enabled) {
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto pos = base_idx + idx;
		D_ASSERT(pos < this->count);
		found_match[pos] = true;
	}
}

void OuterJoinMarker::ConstructLeftJoinResult(DataChunk &left, DataChunk &result) {
	if (!enabled) {
		return;
	}
	D_ASSERT(count == STANDARD_VECTOR_SIZE);
	SelectionVector remaining_sel(STANDARD_VECTOR_SIZE);
	idx_t remaining_count = 0;
	for (idx_t i = 0; i < left.size(); i++) {
		if (!found_match[i]) {
			remaining_sel.set_index(remaining_count++, i);
		}
	}
	if (remaining_count > 0) {
		result.Slice(left, remaining_sel, remaining_count);
		for (idx_t idx = left.ColumnCount(); idx < result.ColumnCount(); idx++) {
			result.data[idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result.data[idx], true);
		}
	}
}

idx_t OuterJoinMarker::MaxThreads() const {
	return count / (STANDARD_VECTOR_SIZE * 10);
}

void OuterJoinMarker::InitializeScan(ColumnDataCollection &data, OuterJoinGlobalScanState &gstate) {
	gstate.data = &data;
	data.InitializeScan(gstate.global_scan);
}

void OuterJoinMarker::InitializeScan(OuterJoinGlobalScanState &gstate, OuterJoinLocalScanState &lstate) {
	D_ASSERT(gstate.data);
	lstate.match_sel.Initialize(STANDARD_VECTOR_SIZE);
	gstate.data->InitializeScanChunk(lstate.scan_chunk);
}

void OuterJoinMarker::Scan(OuterJoinGlobalScanState &gstate, OuterJoinLocalScanState &lstate, DataChunk &result) {
	D_ASSERT(gstate.data);
	// fill in NULL values for the LHS
	while (gstate.data->Scan(gstate.global_scan, lstate.local_scan, lstate.scan_chunk)) {
		idx_t result_count = 0;
		// figure out which tuples didn't find a match in the RHS
		for (idx_t i = 0; i < lstate.scan_chunk.size(); i++) {
			if (!found_match[lstate.local_scan.current_row_index + i]) {
				lstate.match_sel.set_index(result_count++, i);
			}
		}
		if (result_count > 0) {
			// if there were any tuples that didn't find a match, output them
			idx_t left_column_count = result.ColumnCount() - lstate.scan_chunk.ColumnCount();
			for (idx_t i = 0; i < left_column_count; i++) {
				result.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result.data[i], true);
			}
			for (idx_t col_idx = left_column_count; col_idx < result.ColumnCount(); col_idx++) {
				result.data[col_idx].Slice(lstate.scan_chunk.data[col_idx - left_column_count], lstate.match_sel,
				                           result_count);
			}
			result.SetCardinality(result_count);
			return;
		}
	}
}

} // namespace duckdb
