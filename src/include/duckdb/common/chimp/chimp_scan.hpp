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
struct ChimpScanState : public SegmentScanState {
public:
	static constexpr uint32_t CHIMP_SEQUENCE_SIZE = 1024;

	explicit ChimpScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();
		chimp_state.input.SetStream((uint8_t *)dataptr);
	}

	duckdb_chimp::Chimp128DecompressionState chimp_state;
	BufferHandle handle;

	void (*decompress_function)(data_ptr_t, data_ptr_t);

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		// while (skip_count > 0) {
		//	if (position_in_group + skip_count < CHIMP_METADATA_GROUP_SIZE) {
		//		// We're not leaving this chimp group, we can perform all skips.
		//		position_in_group += skip_count;
		//		break;
		//	} else {
		//		// The skip crosses the current chimp group, we skip the remainder of this group.
		//		auto skipping = CHIMP_METADATA_GROUP_SIZE - position_in_group;
		//		position_in_group = 0;
		//		current_metadata_group_ptr += (current_width * CHIMP_METADATA_GROUP_SIZE) / 8;

		//		skip_count -= skipping;
		//	}
		//}
	}

	// void LoadDecompressFunction() {
	//	decompress_function = &ChimpPrimitives::UnPackBlock<T>;
	// }
};

template <class T>
unique_ptr<SegmentScanState> ChimpInitScan(ColumnSegment &segment) {
	auto result = make_unique_base<SegmentScanState, ChimpScanState<T>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void ChimpScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	using INTERNAL_TYPE = typename ChimpType<T>::type;
	auto &scan_state = (ChimpScanState<T> &)*state.scan_state;
	auto &chimp_state = scan_state.chimp_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	idx_t scanned = 0;

	for (idx_t scanned = 0; scanned < scan_count; scanned++) {

		auto current_result_ptr = (INTERNAL_TYPE *)(result_data + result_offset + scanned);
		if (!duckdb_chimp::Chimp128Decompression<INTERNAL_TYPE>::Load(*current_result_ptr, chimp_state)) {
			//! End of stream is reached
			break;
		}
	}
}

template <class T>
void ChimpScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// ChimpScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
