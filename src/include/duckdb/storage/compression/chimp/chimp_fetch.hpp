//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_fetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/storage/compression/chimp/chimp_scan.hpp"

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
void ChimpFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	using INTERNAL_TYPE = typename ChimpType<T>::type;

	ChimpScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);
	auto result_data = FlatVector::GetData<INTERNAL_TYPE>(result);

	if (scan_state.GroupFinished() && scan_state.total_value_count < scan_state.segment_count) {
		scan_state.LoadGroup(scan_state.group_state.values);
	}
	scan_state.group_state.Scan(&result_data[result_idx], 1);

	scan_state.total_value_count++;
}

} // namespace duckdb
