//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/patas_fetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/storage/compression/patas/patas_scan.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

namespace duckdb {

template <class T>
void PatasFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	PatasScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);
	auto result_data = FlatVector::GetData<EXACT_TYPE>(result);
	result_data[result_idx] =
	    patas::PatasDecompression<EXACT_TYPE>::Load(scan_state.patas_state, scan_state.group_state.index);
}

} // namespace duckdb
