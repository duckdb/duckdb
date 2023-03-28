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

#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//! Do not change order of these variables
struct PatasUnpackedValueStats {
	uint8_t significant_bytes;
	uint8_t trailing_zeros;
	uint8_t index_diff;
};

template <class EXACT_TYPE>
struct PatasGroupState {
public:
	void Init(uint8_t *data) {
		byte_reader.SetStream(data);
	}

	idx_t BytesRead() const {
		return byte_reader.Index();
	}

	void Reset() {
		index = 0;
	}

	void LoadPackedData(uint16_t *packed_data, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			auto &unpacked = unpacked_data[i];
			PackedDataUtils<EXACT_TYPE>::Unpack(packed_data[i], (UnpackedData &)unpacked);
		}
	}

	template <bool SKIP = false>
	void Scan(uint8_t *dest, idx_t count) {
		if (!SKIP) {
			memcpy(dest, (void *)(values + index), sizeof(EXACT_TYPE) * count);
		}
		index += count;
	}

	template <bool SKIP>
	void LoadValues(EXACT_TYPE *value_buffer, idx_t count) {
		if (SKIP) {
			return;
		}
		value_buffer[0] = (EXACT_TYPE)0;
		for (idx_t i = 0; i < count; i++) {
			value_buffer[i] = patas::PatasDecompression<EXACT_TYPE>::DecompressValue(
			    byte_reader, unpacked_data[i].significant_bytes, unpacked_data[i].trailing_zeros,
			    value_buffer[i - unpacked_data[i].index_diff]);
		}
	}

public:
	idx_t index;
	PatasUnpackedValueStats unpacked_data[PatasPrimitives::PATAS_GROUP_SIZE];
	EXACT_TYPE values[PatasPrimitives::PATAS_GROUP_SIZE];

private:
	ByteReader byte_reader;
};

template <class T>
struct PatasScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	explicit PatasScanState(ColumnSegment &segment) : segment(segment), count(segment.count) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

		handle = buffer_manager.Pin(segment.block);
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		segment_data = handle.Ptr() + segment.GetBlockOffset();
		auto metadata_offset = Load<uint32_t>(segment_data);
		metadata_ptr = segment_data + metadata_offset;
	}

	BufferHandle handle;
	data_ptr_t metadata_ptr;
	data_ptr_t segment_data;
	idx_t total_value_count = 0;
	PatasGroupState<EXACT_TYPE> group_state;

	ColumnSegment &segment;
	idx_t count;

	idx_t LeftInGroup() const {
		return PatasPrimitives::PATAS_GROUP_SIZE - (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE);
	}

	inline bool GroupFinished() const {
		return (total_value_count % PatasPrimitives::PATAS_GROUP_SIZE) == 0;
	}

	// Scan up to a group boundary
	template <class EXACT_TYPE, bool SKIP = false>
	void ScanGroup(EXACT_TYPE *values, idx_t group_size) {
		D_ASSERT(group_size <= PatasPrimitives::PATAS_GROUP_SIZE);
		D_ASSERT(group_size <= LeftInGroup());

		if (GroupFinished() && total_value_count < count) {
			if (group_size == PatasPrimitives::PATAS_GROUP_SIZE) {
				LoadGroup<SKIP>(values);
				total_value_count += group_size;
				return;
			} else {
				// Even if SKIP is given, group size is not big enough to be able to fully skip the entire group
				LoadGroup<false>(group_state.values);
			}
		}
		group_state.template Scan<SKIP>((uint8_t *)values, group_size);

		total_value_count += group_size;
	}

	// Using the metadata, we can avoid loading any of the data if we don't care about the group at all
	void SkipGroup() {
		// Skip the offset indicating where the data starts
		metadata_ptr -= sizeof(uint32_t);
		idx_t group_size = MinValue((idx_t)PatasPrimitives::PATAS_GROUP_SIZE, count - total_value_count);
		// Skip the blocks of packed data
		metadata_ptr -= sizeof(uint16_t) * group_size;

		total_value_count += group_size;
	}

	template <bool SKIP = false>
	void LoadGroup(EXACT_TYPE *value_buffer) {
		group_state.Reset();

		// Load the offset indicating where a groups data starts
		metadata_ptr -= sizeof(uint32_t);
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		D_ASSERT(data_byte_offset < Storage::BLOCK_SIZE);

		// Initialize the byte_reader with the data values for the group
		group_state.Init(segment_data + data_byte_offset);

		idx_t group_size = MinValue((idx_t)PatasPrimitives::PATAS_GROUP_SIZE, (count - total_value_count));

		// Read the compacted blocks of (7 + 6 + 3 bits) value stats
		metadata_ptr -= sizeof(uint16_t) * group_size;
		group_state.LoadPackedData((uint16_t *)metadata_ptr, group_size);

		// Read all the values to the specified 'value_buffer'
		group_state.template LoadValues<SKIP>(value_buffer, group_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		using EXACT_TYPE = typename FloatingToExact<T>::type;

		if (total_value_count != 0 && !GroupFinished()) {
			// Finish skipping the current group
			idx_t to_skip = LeftInGroup();
			skip_count -= to_skip;
			ScanGroup<EXACT_TYPE, true>(nullptr, to_skip);
		}
		// Figure out how many entire groups we can skip
		// For these groups, we don't even need to process the metadata or values
		idx_t groups_to_skip = skip_count / PatasPrimitives::PATAS_GROUP_SIZE;
		for (idx_t i = 0; i < groups_to_skip; i++) {
			SkipGroup();
		}
		skip_count -= PatasPrimitives::PATAS_GROUP_SIZE * groups_to_skip;
		if (skip_count == 0) {
			return;
		}
		// For the last group that this skip (partially) touches, we do need to
		// load the metadata and values into the group_state
		ScanGroup<EXACT_TYPE, true>(nullptr, skip_count);
	}
};

template <class T>
unique_ptr<SegmentScanState> PatasInitScan(ColumnSegment &segment) {
	auto result = make_uniq_base<SegmentScanState, PatasScanState<T>>(segment);
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

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetData<EXACT_TYPE>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	current_result_ptr += result_offset;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		const auto remaining = scan_count - scanned;
		const idx_t to_scan = MinValue(remaining, scan_state.LeftInGroup());

		scan_state.template ScanGroup<EXACT_TYPE>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
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
