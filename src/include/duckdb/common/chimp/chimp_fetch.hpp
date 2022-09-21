//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitpacking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/chimp/chimp.hpp"

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
void ChimpFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	// ChimpScanState<T> scan_state(segment);
	// scan_state.Skip(segment, row_id);
	// auto result_data = FlatVector::GetData<T>(result);
	// T *current_result_ptr = result_data + result_idx;

	//// TODO clean up, is reused in partialscan
	// idx_t offset_in_compression_group =
	//     scan_state.position_in_group % ChimpPrimitives::CHIMP_ALGORITHM_GROUP_SIZE;

	// data_ptr_t decompression_group_start_pointer =
	//     scan_state.current_metadata_group_ptr +
	//     (scan_state.position_in_group - offset_in_compression_group) * scan_state.current_width / 8;

	////! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	// bool skip_sign_extend = true;

	// scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
	//                                scan_state.current_width, skip_sign_extend);

	//*current_result_ptr = *(T *)(scan_state.decompression_buffer + offset_in_compression_group);
	////! Apply FOR to result
	//*current_result_ptr += scan_state.current_frame_of_reference;
}
template <class T>
void ChimpSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	// auto &scan_state = (ChimpScanState<T> &)*state.scan_state;
	// scan_state.Skip(segment, skip_count);
}

} // namespace duckdb
