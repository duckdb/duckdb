//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_fetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alprd/alprd_scan.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

template <class T>
void AlpRDFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	D_ASSERT(row_id >= 0);
	auto row_index = NumericCast<idx_t>(row_id);
	D_ASSERT(row_index < segment.count);
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(state.context, segment.GetBlockHandle());
	AlpRDScanState<T> scan_state(std::move(handle), segment);
	scan_state.Skip(segment, row_index);
	auto result_data = FlatVector::GetDataMutableUnsafe<EXACT_TYPE>(result);
	result_data[result_idx] = (EXACT_TYPE)0;

	if (scan_state.VectorFinished() && scan_state.total_value_count < scan_state.count) {
		scan_state.LoadVector(scan_state.vector_state.decoded_values);
	}
	scan_state.vector_state.Scan((uint8_t *)(result_data + result_idx), 1);
	scan_state.total_value_count++;
}

} // namespace duckdb
