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
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

template <class T>
void AlpRDFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	AlpRDScanState<T> scan_state(segment);
	scan_state.Skip(segment, UnsafeNumericCast<idx_t>(row_id));
	auto result_data = FlatVector::GetData<EXACT_TYPE>(result);
	result_data[result_idx] = (EXACT_TYPE)0;

	if (scan_state.VectorFinished() && scan_state.total_value_count < scan_state.count) {
		scan_state.LoadVector(scan_state.vector_state.decoded_values);
	}
	scan_state.vector_state.Scan((uint8_t *)(result_data + result_idx), 1);
	scan_state.total_value_count++;
}

} // namespace duckdb
