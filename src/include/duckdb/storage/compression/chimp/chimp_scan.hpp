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

#include "duckdb/storage/compression/chimp/algorithm/flag_buffer.hpp"
#include "duckdb/storage/compression/chimp/algorithm/leading_zero_buffer.hpp"

namespace duckdb {

struct ChimpGroupState {
public:
	bool Started() const {
		return !!index;
	}
	uint8_t GetFlag() {
		D_ASSERT(index <= max_flags_to_read);
		D_ASSERT(index <= 1024);
		return flags[index++];
	}
	// Assuming the group is completely full
	idx_t RemainingInGroup() const {
		return ChimpPrimitives::CHIMP_SEQUENCE_SIZE - index;
	}

	void LoadFlags(uint8_t *packed_data, idx_t group_size) {
		duckdb_chimp::FlagBuffer<false> flag_buffer;
		flag_buffer.SetBuffer(packed_data);
		flags[0] = 0; // First value doesn't require a flag
		for (idx_t i = 0; i < group_size; i++) {
			flags[1 + i] = flag_buffer.Extract();
		}
		D_ASSERT(group_size != 0);
		max_flags_to_read = group_size;
		index = 0;
	}
	void LoadLeadingZeros(uint8_t *packed_data, idx_t leading_zero_block_size) {
#ifdef DEBUG
		idx_t flag_one_count = 0;
		for (idx_t i = 0; i < max_flags_to_read; i++) {
			flag_one_count += flags[1 + i] == duckdb_chimp::LEADING_ZERO_LOAD;
		}
		// There are 8 leading zero values packed in one block, the block could not be entire filled
		flag_one_count = AlignValue<idx_t, 8>(flag_one_count);
		D_ASSERT(flag_one_count == leading_zero_block_size);
#endif
		duckdb_chimp::LeadingZeroBuffer<false> leading_zero_buffer;
		leading_zero_buffer.SetBuffer(packed_data);
		for (idx_t i = 0; i < leading_zero_block_size; i++) {
			leading_zeros[i] =
			    duckdb_chimp::ChimpDecompressionConstants::LEADING_REPRESENTATION[leading_zero_buffer.Extract()];
		}
		max_leading_zeros_to_read = leading_zero_block_size;
		leading_zero_index = 0;
	}

	idx_t CalculatePackedDataCount() const {
		idx_t count = 0;
		for (idx_t i = 0; i < max_flags_to_read; i++) {
			count += flags[1 + i] == duckdb_chimp::TRAILING_EXCEEDS_THRESHOLD;
		}
		return count;
	}

	template <class CHIMP_TYPE>
	void LoadPackedData(uint16_t *packed_data, idx_t packed_data_block_count) {
		for (idx_t i = 0; i < packed_data_block_count; i++) {
			duckdb_chimp::PackedDataUtils<CHIMP_TYPE>::Unpack(packed_data[i], unpacked_data_blocks[i]);
		}
		unpacked_index = 0;
		max_packed_data_to_read = packed_data_block_count;
	}

public:
	idx_t index;
	uint8_t flags[ChimpPrimitives::CHIMP_SEQUENCE_SIZE + 1];
	uint8_t leading_zeros[ChimpPrimitives::CHIMP_SEQUENCE_SIZE + 1];
	uint32_t leading_zero_index;
	uint32_t unpacked_index;
	duckdb_chimp::UnpackedData unpacked_data_blocks[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];
	idx_t max_leading_zeros_to_read;

private:
	idx_t max_flags_to_read;
	idx_t max_packed_data_to_read;
};

template <class T>
struct ChimpScanState : public SegmentScanState {
public:
	using CHIMP_TYPE = typename ChimpType<T>::type;

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

	duckdb_chimp::Chimp128DecompressionState<CHIMP_TYPE> chimp_state;
	BufferHandle handle;
	data_ptr_t metadata_ptr;
	idx_t total_value_count = 0;
	ChimpGroupState group_state;

	ColumnSegment &segment;

	idx_t LeftInGroup() const {
		return ChimpPrimitives::CHIMP_SEQUENCE_SIZE - (total_value_count % ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
	}

	bool GroupFinished() const {
		return (total_value_count % ChimpPrimitives::CHIMP_SEQUENCE_SIZE) == 0;
	}

	// Scan a group from the start
	template <class CHIMP_TYPE>
	void ScanGroup(CHIMP_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		values[0] = duckdb_chimp::Chimp128Decompression<CHIMP_TYPE>::LoadFirst(chimp_state);
		for (idx_t i = 1; i < group_size; i++) {
			values[i] = duckdb_chimp::Chimp128Decompression<CHIMP_TYPE>::DecompressValue(
			    group_state.flags[i], group_state.leading_zeros, group_state.leading_zero_index,
			    group_state.unpacked_data_blocks, group_state.unpacked_index, chimp_state);
			D_ASSERT(group_state.leading_zero_index <= group_state.max_leading_zeros_to_read);
		}
		group_state.index += group_size;
		total_value_count += group_size;
		if (GroupFinished() && total_value_count < segment.count) {
			LoadGroup();
		}
	}

	// Scan up to a group boundary
	template <class CHIMP_TYPE>
	void ScanPartialGroup(CHIMP_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		for (idx_t i = 0; i < group_size; i++) {
			values[i] = duckdb_chimp::Chimp128Decompression<CHIMP_TYPE>::Load(
			    group_state.flags[group_state.index + i], group_state.leading_zeros, group_state.leading_zero_index,
			    group_state.unpacked_data_blocks, group_state.unpacked_index, chimp_state);
		}
		group_state.index += group_size;
		total_value_count += group_size;
		if (GroupFinished() && total_value_count < segment.count) {
			LoadGroup();
		}
	}

	void LoadGroup() {
		chimp_state.Reset();

		//! FIXME: If we change the order of this to flag -> leading_zero_blocks -> packed_data
		//! We can leave out the leading zero block count as well, because it can be derived from
		//! Extracting all the flags and counting the 3's

		// Load the offset indicating where a groups data starts
		metadata_ptr -= sizeof(uint32_t);
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		D_ASSERT(data_byte_offset < Storage::BLOCK_SIZE);
		//  Only used for point queries
		(void)data_byte_offset;

		// Load how many blocks of leading zero bits we have
		metadata_ptr -= sizeof(uint8_t);
		auto leading_zero_block_count = Load<uint8_t>(metadata_ptr);
		D_ASSERT(leading_zero_block_count <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 8);

		// Load the leading zero block count
		metadata_ptr -= 3 * leading_zero_block_count;
		const auto leading_zero_block_ptr = metadata_ptr;

		// Figure out how many flags there are
		D_ASSERT(segment.count >= total_value_count);
		uint16_t flag_count =
		    duckdb::MinValue<idx_t>(segment.count - total_value_count, ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
		// Reduce by one, because the first value of a group does not have a flag
		flag_count--;
		uint16_t flag_byte_count = (AlignValue<uint16_t, 4>(flag_count) / 4);

		// Load the flags
		metadata_ptr -= flag_byte_count;
		auto flags = metadata_ptr;
		group_state.LoadFlags(flags, flag_count);

		// Load the leading zero blocks
		group_state.LoadLeadingZeros(leading_zero_block_ptr, (uint32_t)leading_zero_block_count * 8);

		// Load packed data blocks
		auto packed_data_block_count = group_state.CalculatePackedDataCount();
		metadata_ptr -= packed_data_block_count * 2;
		if ((uint64_t)metadata_ptr & 1) {
			// Align on a two-byte boundary
			metadata_ptr--;
		}
		group_state.LoadPackedData<CHIMP_TYPE>((uint16_t *)metadata_ptr, packed_data_block_count);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	// TODO: use the metadata to determine if we can skip a group
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using INTERNAL_TYPE = typename ChimpType<T>::type;
		INTERNAL_TYPE buffer[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

		while (skip_count) {
			auto skip_size = std::min(skip_count, LeftInGroup());
			if (!group_state.Started()) {
				ScanGroup(buffer, skip_size);
			} else {
				ScanPartialGroup(buffer, skip_size);
			}
			skip_count -= skip_size;
		}
	}
};

template <class T>
unique_ptr<SegmentScanState> ChimpInitScan(ColumnSegment &segment) {
	auto result = make_unique_base<SegmentScanState, ChimpScanState<T>>(segment);
	return result;
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

	auto current_result_ptr = (INTERNAL_TYPE *)(result_data + result_offset);

	auto scanned = std::min(scan_count, scan_state.LeftInGroup());

	if (!scan_state.group_state.Started()) {
		scan_state.template ScanGroup<INTERNAL_TYPE>(current_result_ptr, scanned);
	} else {
		scan_state.template ScanPartialGroup<INTERNAL_TYPE>(current_result_ptr, scanned);
	}
	scan_count -= scanned;
	if (!scan_count) {
		//! Already scanned everything
		return;
	}
	// We know for sure that the last group has ended
	D_ASSERT(!scan_state.group_state.Started());
	while (scan_count) {
		auto to_scan = duckdb::MinValue<idx_t>(scan_count, ChimpPrimitives::CHIMP_SEQUENCE_SIZE);
		scan_state.template ScanGroup<INTERNAL_TYPE>(current_result_ptr + scanned, to_scan);
		scan_count -= to_scan;
		scanned += to_scan;
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
