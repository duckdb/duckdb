//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/chimp/chimp_scan.hpp
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
	explicit ChimpScanState(ColumnSegment &segment) : segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		auto start_of_data_segment = dataptr + segment.GetBlockOffset() + ChimpPrimitives::HEADER_SIZE;
		chimp_state.input.SetStream(start_of_data_segment);
		auto metadata_offset = Load<uint32_t>(dataptr + segment.GetBlockOffset());
		metadata_ptr = dataptr + segment.GetBlockOffset() + metadata_offset;
		LoadGroup();
	}

	duckdb_chimp::Chimp128DecompressionState chimp_state;
	BufferHandle handle;
	data_ptr_t metadata_ptr;
	idx_t group_idx = 0;
	idx_t total_value_count = 0;
	ColumnSegment &segment;

	template <class CHIMP_TYPE>
	bool ScanSingle(CHIMP_TYPE &value) {
		bool result = duckdb_chimp::Chimp128Decompression<CHIMP_TYPE>::Load(value, chimp_state);
		group_idx++;
		if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			chimp_state.Reset();
			total_value_count += group_idx;
			group_idx = 0;
			LoadGroup();
		}
		return result;
	}

	void LoadGroup() {
		// Load the offset indicating where a groups data starts
		metadata_ptr -= sizeof(uint32_t);
		auto data_bit_offset = Load<uint32_t>(metadata_ptr);
		//  Only used for point queries
		(void)data_bit_offset;

		// Load how many blocks of leading zero bits we have
		metadata_ptr -= sizeof(uint8_t);
		auto leading_zero_block_count = Load<uint8_t>(metadata_ptr);

		// Load the leading zero blocks
		metadata_ptr -= 3 * leading_zero_block_count;
		chimp_state.leading_zero_buffer.SetBuffer(metadata_ptr);

		// Load how many flag bytes there are
		metadata_ptr -= sizeof(uint16_t);
		auto size_of_group = Load<uint16_t>(metadata_ptr);

		// Load the flags
		metadata_ptr -= size_of_group;
		chimp_state.flag_buffer.SetBuffer(metadata_ptr);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	// TODO: use the metadata to determine if we can skip a group
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using INTERNAL_TYPE = typename ChimpType<T>::type;
		INTERNAL_TYPE unused;

		for (idx_t i = 0; i < skip_count; i++) {

			// We still need to run through all the values to record them in the ring buffer
			if (!ScanSingle(unused)) {
				//! End of stream is reached
				break;
			}
		}
	}
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

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	for (idx_t scanned = 0; scanned < scan_count; scanned++) {

		auto current_result_ptr = (INTERNAL_TYPE *)(result_data + result_offset + scanned);
		if (!scan_state.template ScanSingle<INTERNAL_TYPE>(*current_result_ptr)) {
			//! End of stream is reached
			break;
		}
	}
}

template <class T>
void ChimpSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (ChimpScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void ChimpScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	ChimpScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
