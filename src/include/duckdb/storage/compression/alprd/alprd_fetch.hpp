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
	AlpRDScanState<T> scan_state(segment);
	scan_state.Skip(segment, UnsafeNumericCast<idx_t>(row_id));
	auto result_data = FlatVector::GetDataUnsafe<EXACT_TYPE>(result);
	result_data[result_idx] = (EXACT_TYPE)0;

	if (scan_state.VectorFinished() && scan_state.total_value_count < scan_state.count) {
		scan_state.LoadVector(scan_state.vector_state.decoded_values);
	}
	scan_state.vector_state.Scan((uint8_t *)(result_data + result_idx), 1);
	scan_state.total_value_count++;
}

} // namespace duckdb
