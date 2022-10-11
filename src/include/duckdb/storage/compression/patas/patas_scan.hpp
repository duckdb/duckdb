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
		stored_previous_values = 0;
	}

	// If we read (for instance) 40 values, cache that and then read 40 more values, we have to add to the cache, not
	// just replace the old one We have to assume that 'stored_previous_values' is reset to 0 every time a group ends,
	// then we can use the 'count' and the 'stored_previous_values' to determine whether we need to override the old
	// values Actually, we might need to memmove the previous_values if count < 128, and stored_previous_values != 0
	void CachePreviousValues(EXACT_TYPE *values, idx_t count) {
		D_ASSERT(count <= patas::BUFFER_SIZE);
		const idx_t old_count = stored_previous_values;
		// Limit the previous values count to 128
		stored_previous_values = std::min((idx_t)patas::BUFFER_SIZE, stored_previous_values + count);
		idx_t insert_at_index = 0;
		if (old_count && count < patas::BUFFER_SIZE && old_count + count > patas::BUFFER_SIZE) {
			// Old + new is bigger than the buffer, some of 'old' can still be referenced: move it back
			const idx_t useful_values = (old_count + count) - patas::BUFFER_SIZE;
			memmove(previous_values, previous_values + useful_values, (old_count - useful_values) * sizeof(EXACT_TYPE));
			insert_at_index = (old_count - useful_values);
		}
		memcpy(previous_values + insert_at_index, values, count * sizeof(EXACT_TYPE));
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

	EXACT_TYPE previous_values[patas::BUFFER_SIZE];
	idx_t stored_previous_values;
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
	template <class EXACT_TYPE>
	void ScanPartialGroup(EXACT_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= PatasPrimitives::PATAS_GROUP_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		// Set the first value to 0, because the first value of a group uses this as reference
		values[0] = (EXACT_TYPE)0;

		// First load the values that (could) require cached values
		for (idx_t i = 0; i < patas::BUFFER_SIZE && i < group_size; i++) {
			const uint8_t index_diff = group_state.index_diffs[group_state.index + i];
			if (index_diff > i) {
				D_ASSERT((index_diff - i) <= group_state.stored_previous_values);
				// Have to use the cached previous values
				const auto cache_index = group_state.stored_previous_values - (index_diff - i);
				values[i] = patas::PatasDecompression<EXACT_TYPE>::Load(
				    patas_state, group_state.index + i, group_state.byte_counts, group_state.trailing_zeros,
				    group_state.previous_values[cache_index]);
			} else {
				values[i] = patas::PatasDecompression<EXACT_TYPE>::Load(
				    patas_state, group_state.index + i, group_state.byte_counts, group_state.trailing_zeros,
				    values[i - index_diff]);
			}
		}
		// After that we can use the values idx
		for (idx_t i = std::min((idx_t)patas::BUFFER_SIZE, group_size); i < group_size; i++) {
			const auto index_diff = group_state.index_diffs[group_state.index + i];
			values[i] =
			    patas::PatasDecompression<EXACT_TYPE>::Load(patas_state, group_state.index + i, group_state.byte_counts,
			                                                group_state.trailing_zeros, values[i - index_diff]);
		}
		group_state.index += group_size;
		total_value_count += group_size;
		if (GroupFinished() && total_value_count < segment.count) {
			LoadGroup();
		} else {
			auto to_cache = std::min(group_size, (idx_t)patas::BUFFER_SIZE);
			group_state.CachePreviousValues(values + (group_size - to_cache), to_cache);
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
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	// TODO: use the metadata to determine if we can skip a group
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using EXACT_TYPE = typename FloatingToExact<T>::type;
		EXACT_TYPE buffer[PatasPrimitives::PATAS_GROUP_SIZE];

		while (skip_count) {
			auto skip_size = std::min(skip_count, LeftInGroup());
			ScanPartialGroup(buffer, skip_size);
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

	auto current_group_remainder = std::min(scan_count, scan_state.LeftInGroup());
	scan_count -= current_group_remainder;
	auto iterations = scan_count / PatasPrimitives::PATAS_GROUP_SIZE;
	auto remainder = scan_count % PatasPrimitives::PATAS_GROUP_SIZE;

	scan_state.template ScanPartialGroup<EXACT_TYPE>(current_result_ptr, current_group_remainder);

	for (idx_t i = 0; i < iterations; i++) {
		scan_state.template ScanPartialGroup<EXACT_TYPE>(current_result_ptr + current_group_remainder +
		                                                     (i * PatasPrimitives::PATAS_GROUP_SIZE),
		                                                 PatasPrimitives::PATAS_GROUP_SIZE);
	}
	if (remainder) {
		scan_state.template ScanPartialGroup<EXACT_TYPE>(
		    current_result_ptr + current_group_remainder + (iterations * PatasPrimitives::PATAS_GROUP_SIZE), remainder);
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
