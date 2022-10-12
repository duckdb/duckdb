//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/storage/compression/chimp/chimp_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/storage/compression/chimp/algorithm/chimp_utils.hpp"

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

using duckdb_chimp::SignificantBits;

template <class EXACT_TYPE>
struct PatasGroupState {
public:
	bool Started() const {
		return !!index;
	}
	void Reset() {
		index = 0;
		previous_values[0] = (EXACT_TYPE)0;
	}

	// Assuming the group is completely full
	idx_t RemainingInGroup() const {
		return PatasPrimitives::PATAS_GROUP_SIZE - index;
	}
	void LoadByteCounts(uint8_t *bitpacked_data, idx_t block_count) {
		//! Unpack 'count' values of bitpacked data, unpacked per group of 32 values
		const auto value_count = block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		BitpackingPrimitives::UnPackBuffer<uint8_t>(byte_counts, bitpacked_data, value_count,
		                                            PatasPrimitives::BYTECOUNT_BITSIZE);
	}
	void LoadTrailingZeros(uint8_t *bitpacked_data, idx_t block_count) {
		const auto value_count = block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		BitpackingPrimitives::UnPackBuffer<uint8_t>(trailing_zeros, bitpacked_data, value_count,
		                                            SignificantBits<EXACT_TYPE>::size);
	}
	void LoadIndexDifferences(uint8_t *bitpacked_data, idx_t block_count) {
		const auto value_count = block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		BitpackingPrimitives::UnPackBuffer<uint8_t>(index_diffs, bitpacked_data, value_count,
		                                            PatasPrimitives::INDEX_BITSIZE);
	}

public:
	idx_t index;
	uint8_t trailing_zeros[PatasPrimitives::PATAS_GROUP_SIZE];
	uint8_t byte_counts[PatasPrimitives::PATAS_GROUP_SIZE];
	uint8_t index_diffs[PatasPrimitives::PATAS_GROUP_SIZE];
	EXACT_TYPE previous_values[PatasPrimitives::PATAS_GROUP_SIZE];
};

template <class T>
struct PatasScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	explicit PatasScanState(ColumnSegment &segment) : segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		auto start_of_data_segment = dataptr + segment.GetBlockOffset() + PatasPrimitives::HEADER_SIZE;
		patas_state.byte_reader.SetStream(start_of_data_segment);
		auto metadata_offset = Load<uint32_t>(dataptr + segment.GetBlockOffset());
		metadata_ptr = dataptr + segment.GetBlockOffset() + metadata_offset;
		LoadGroup();
	}

	patas::PatasDecompressionState<EXACT_TYPE> patas_state;
	BufferHandle handle;
	data_ptr_t metadata_ptr;
	idx_t total_value_count = 0;
	PatasGroupState<EXACT_TYPE> group_state;

	ColumnSegment &segment;

	idx_t LeftInGroup() const {
		return PatasPrimitives::PATAS_GROUP_SIZE - (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE);
	}

	bool GroupFinished() const {
		return (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE) == 0;
	}

	//	// Scan a group from the start
	//	template <class EXACT_TYPE>
	//	void ScanGroup(EXACT_TYPE *values, idx_t group_size) {
	//		D_ASSERT(group_size <= PatasPrimitives::PATAS_GROUP_SIZE);
	//		D_ASSERT(group_size <= LeftInGroup());

	//#ifdef DEBUG
	//		const auto old_data_size = patas_state.byte_reader.Index();
	//#endif
	//		values[0] = patas::PatasDecompression<EXACT_TYPE>::LoadFirst(patas_state);
	//		for (idx_t i = 1; i < group_size; i++) {
	//			values[i] = patas::PatasDecompression<EXACT_TYPE>::DecompressValue(patas_state);
	//		}
	//		group_state.index += group_size;
	//		total_value_count += group_size;
	//#ifdef DEBUG
	//		idx_t expected_change = 0;
	//		idx_t start_idx = group_state.index - group_size;
	//		for (idx_t i = 0; i < group_size; i++) {
	//			uint8_t byte_count = patas_state.byte_counts[start_idx + i];
	//			uint8_t trailing_zeros = patas_state.trailing_zeros[start_idx + i];
	//			if (byte_count == 0) {
	//				byte_count += sizeof(EXACT_TYPE);
	//			}
	//			if (byte_count == 1 && trailing_zeros == 0) {
	//				byte_count -= 1;
	//			}
	//			expected_change += byte_count;
	//		}
	//		D_ASSERT(patas_state.byte_reader.Index() >= old_data_size);
	//		D_ASSERT(expected_change == (patas_state.byte_reader.Index() - old_data_size));
	//#endif
	//		if (GroupFinished() && total_value_count < segment.count) {
	//			LoadGroup();
	//		}
	//		// if (total_value_count == segment.count){
	//		// printf("DECOMPRESS: DATA BYTES SIZE: %llu\n", patas_state.byte_reader.Index());
	//		//}
	//	}

	// Scan up to a group boundary
	template <class EXACT_TYPE, bool FROM_START = false>
	void ScanGroup(EXACT_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= PatasPrimitives::PATAS_GROUP_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		if (FROM_START) {
			D_ASSERT(!group_state.Started());
			D_ASSERT(group_state.index == 0);

			// Since we are scanning from the start, we can use the values array as our 'previous_values'
			for (idx_t i = 0; i < group_size; i++) {
				const auto index_diff = group_state.index_diffs[i];
				D_ASSERT(index_diff <= i);
				values[i] = patas::PatasDecompression<EXACT_TYPE>::Load(patas_state, i, group_state.byte_counts,
				                                                        group_state.trailing_zeros,
				                                                        (i != 0) * values[i - index_diff]);
				group_state.previous_values[i] = values[i];
			}
			if (!GroupFinished()) {
				// We don't need to copy over the values to 'previous_values' if the group has already ended
				// Even then, we're only interested in the last 128 values, so we can avoid copying most of these
				const idx_t previous_value_count = MinValue((idx_t)PatasPrimitives::PATAS_GROUP_SIZE, group_size);
				const idx_t start_index = group_size - previous_value_count;
				memcpy(group_state.previous_values + start_index, values + start_index,
				       sizeof(EXACT_TYPE) * previous_value_count);
			}
		} else {
			// We need to reference and update the 'previous_values' array directly,
			// because we are not scanning from the start of a group
			for (idx_t i = 0; i < group_size; i++) {
				const auto index_diff = group_state.index_diffs[group_state.index + i];
				values[i] = patas::PatasDecompression<EXACT_TYPE>::Load(
				    patas_state, group_state.index + i, group_state.byte_counts, group_state.trailing_zeros,
				    ((group_state.index) + i != 0) * group_state.previous_values[group_state.index + i - index_diff]);
				group_state.previous_values[group_state.index + i] = values[i];
			}
		}
		group_state.index += group_size;
		total_value_count += group_size;
		if (GroupFinished() && total_value_count < segment.count) {
			LoadGroup();
		}
	}

	void LoadGroup() {
		group_state.Reset();

		// Load the offset indicating where a groups data starts
		metadata_ptr -= sizeof(uint32_t);
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		D_ASSERT(data_byte_offset < Storage::BLOCK_SIZE);
		//  Only used for point queries
		(void)data_byte_offset;

		// Load how many blocks of bitpacked data we have
		metadata_ptr -= sizeof(uint8_t);
		auto bitpacked_block_count = Load<uint8_t>(metadata_ptr);
		D_ASSERT(bitpacked_block_count <=
		         PatasPrimitives::PATAS_GROUP_SIZE / BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE);

		const uint64_t trailing_zeros_bits =
		    (SignificantBits<EXACT_TYPE>::size * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		const uint64_t byte_counts_bits =
		    (PatasPrimitives::BYTECOUNT_BITSIZE * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		const uint64_t index_diff_bits =
		    (PatasPrimitives::INDEX_BITSIZE * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		metadata_ptr -= AlignValue(trailing_zeros_bits) / 8;
		// Unpack and store the trailing zeros for the entire group
		group_state.LoadTrailingZeros(metadata_ptr, bitpacked_block_count);

		metadata_ptr -= AlignValue(byte_counts_bits) / 8;
		// Unpack and store the byte counts for the entire group
		group_state.LoadByteCounts(metadata_ptr, bitpacked_block_count);

		metadata_ptr -= AlignValue(index_diff_bits) / 8;
		// Unpack and store the index differences for the entire group
		group_state.LoadIndexDifferences(metadata_ptr, bitpacked_block_count);

		// First value of a group references this
		group_state.previous_values[0] = (EXACT_TYPE)0;
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	// TODO: use the metadata to determine if we can skip a group
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using EXACT_TYPE = typename FloatingToExact<T>::type;
		EXACT_TYPE buffer[PatasPrimitives::PATAS_GROUP_SIZE];

		while (skip_count) {
			auto skip_size = std::min(skip_count, LeftInGroup());
			ScanGroup(buffer, skip_size);
			skip_count -= skip_size;
		}
	}
};

template <class T>
unique_ptr<SegmentScanState> PatasInitScan(ColumnSegment &segment) {
	auto result = make_unique_base<SegmentScanState, PatasScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void PatasScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	auto &scan_state = (PatasScanState<T> &)*state.scan_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto current_result_ptr = (EXACT_TYPE *)(result_data + result_offset);

	idx_t scanned = 0;

	// while (scanned < scan_count) {
	//	const idx_t to_scan = MinValue(scan_count - scanned, scan_state.LeftInGroup());

	//	scan_state.template ScanGroup<EXACT_TYPE>(current_result_ptr + scanned, to_scan);

	//	scanned += to_scan;
	//}
	const auto last_group_remainder = MinValue(scan_count, scan_state.LeftInGroup());
	if (!scan_state.group_state.Started()) {
		scan_state.template ScanGroup<EXACT_TYPE, true>(current_result_ptr, last_group_remainder);
	} else {
		scan_state.template ScanGroup<EXACT_TYPE>(current_result_ptr, last_group_remainder);
	}

	scanned += last_group_remainder;
	const idx_t full_group_iterations = (scan_count - scanned) / PatasPrimitives::PATAS_GROUP_SIZE;

	for (idx_t i = 0; i < full_group_iterations; i++) {
		scan_state.template ScanGroup<EXACT_TYPE, true>(
		    current_result_ptr + scanned + (i * PatasPrimitives::PATAS_GROUP_SIZE), PatasPrimitives::PATAS_GROUP_SIZE);
	}

	scanned += full_group_iterations * PatasPrimitives::PATAS_GROUP_SIZE;
	if (scanned < scan_count) {
		scan_state.template ScanGroup<EXACT_TYPE>(current_result_ptr + scanned, (scan_count - scanned));
	}
}

template <class T>
void PatasSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (PatasScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void PatasScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	PatasScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
