//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alp/alp.hpp"
#include "duckdb/storage/compression/alp/algorithm/alp.hpp"

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

template <class T>
struct AlpGroupState {
public:

	void Reset() {
		index = 0;
	}

	// This is the scan of the data itself, values must have the decoded vector
	template <bool SKIP = false>
	void Scan(uint8_t *dest, idx_t count) {
		if (!SKIP) {
			memcpy(dest, (void *)(values + index), sizeof(T) * count);
		}
		index += count;
	}

	template <bool SKIP>
	void LoadValues(T *value_buffer, idx_t count) {
		if (SKIP) {
			return;
		}
		value_buffer[0] = (T)0;
		// TODO: Here you must call decompression on the vector
		alp::AlpDecompression<T>::Decompress(
		    for_encoded, value_buffer, count, v_factor, v_exponent,
		    exceptions_count, exceptions, exceptions_positions, frame_of_reference, bit_width);
	}

public:
	idx_t index;
	T values[AlpConstants::ALP_VECTOR_SIZE];
	T exceptions[AlpConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpConstants::ALP_VECTOR_SIZE];
	uint8_t for_encoded[AlpConstants::ALP_VECTOR_SIZE * 8]; //! Make room for decompression
	uint8_t v_exponent;
	uint8_t v_factor;
	uint16_t exceptions_count;
	uint64_t frame_of_reference;
	uint8_t bit_width;

};

template <class T>
struct AlpScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	explicit AlpScanState(ColumnSegment &segment) : segment(segment), count(segment.count) {
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
	AlpGroupState<T> group_state;

	ColumnSegment &segment;
	idx_t count;

	idx_t LeftInGroup() const {
		return AlpConstants::ALP_VECTOR_SIZE - (total_value_count % AlpConstants::ALP_VECTOR_SIZE);
	}

	inline bool GroupFinished() const {
		return (total_value_count % AlpConstants::ALP_VECTOR_SIZE) == 0;
	}

	// Scan up to a group boundary
	template <class EXACT_TYPE, bool SKIP = false>
	void ScanGroup(T *values, idx_t group_size) {
		D_ASSERT(group_size <= AlpConstants::ALP_VECTOR_SIZE);
		D_ASSERT(group_size <= LeftInGroup());
		//printf("ScanGroup of %ld\n", group_size);
		if (GroupFinished() && total_value_count < count) {
			if (group_size == AlpConstants::ALP_VECTOR_SIZE) {
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
		idx_t group_size = MinValue((idx_t)AlpConstants::ALP_VECTOR_SIZE, count - total_value_count);
		total_value_count += group_size;
	}

	template <bool SKIP = false>
	void LoadGroup(T *value_buffer) {
		group_state.Reset();

		// Load the offset indicating where a groups data starts
		metadata_ptr -= sizeof(uint32_t);
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		printf("data_byte_offset %d\n", data_byte_offset);
		D_ASSERT(data_byte_offset < Storage::BLOCK_SIZE);

		idx_t group_size = MinValue((idx_t)AlpConstants::ALP_VECTOR_SIZE, (count - total_value_count));

		data_ptr_t group_ptr = segment_data + data_byte_offset;
		group_state.v_exponent = Load<uint8_t>(group_ptr);
		group_ptr += sizeof(uint8_t);
		group_state.v_factor = Load<uint8_t>(group_ptr);
		group_ptr += sizeof(uint8_t);
		group_state.exceptions_count = Load<uint16_t>(group_ptr);
		group_ptr += sizeof(uint16_t);
		group_state.frame_of_reference = Load<uint64_t>(group_ptr);
		group_ptr += sizeof(uint64_t);
		group_state.bit_width = Load<uint8_t>(group_ptr);
		group_ptr += sizeof(uint8_t);

		//printf("v_exponent %d\n", group_state.v_exponent);
		//printf("v_factor %d\n", group_state.v_factor);
		//printf("for %ld\n", group_state.frame_of_reference);
		//printf("bit_width %d\n", group_state.bit_width);

		D_ASSERT(group_state.exceptions_count <= group_size);
		D_ASSERT(group_state.v_exponent <= AlpPrimitives<T>::MAX_EXPONENT);
		D_ASSERT(group_state.v_factor <= group_state.v_exponent);
		D_ASSERT(group_state.bit_width <= sizeof(uint64_t) * 8);

		if (group_state.bit_width > 0){
			auto bp_size = BitpackingPrimitives::GetRequiredSize(group_size, group_state.bit_width);
			memcpy(group_state.for_encoded, (void*) group_ptr, bp_size);
			group_ptr += bp_size;
		}

		if (group_state.exceptions_count > 0){
			memcpy(group_state.exceptions, (void*) group_ptr, sizeof(EXACT_TYPE) * group_state.exceptions_count);
			group_ptr += sizeof(EXACT_TYPE) * group_state.exceptions_count;
			memcpy(group_state.exceptions_positions, (void*) group_ptr, sizeof(uint16_t) * group_state.exceptions_count);
			// group_ptr += sizeof(uint16_t) * group_state.exceptions_count; // TODO: Not needed probably
		}

		// Read all the values to the specified 'value_buffer'
		group_state.template LoadValues<SKIP>(value_buffer, group_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &col_segment, idx_t skip_count) {

		if (total_value_count != 0 && !GroupFinished()) {
			// Finish skipping the current group
			idx_t to_skip = LeftInGroup();
			skip_count -= to_skip;
			ScanGroup<T, true>(nullptr, to_skip);
		}
		// Figure out how many entire groups we can skip
		// For these groups, we don't even need to process the metadata or values
		idx_t groups_to_skip = skip_count / AlpConstants::ALP_VECTOR_SIZE;
		for (idx_t i = 0; i < groups_to_skip; i++) {
			SkipGroup();
		}
		skip_count -= AlpConstants::ALP_VECTOR_SIZE * groups_to_skip;
		if (skip_count == 0) {
			return;
		}
		// For the last group that this skip (partially) touches, we do need to
		// load the metadata and values into the group_state because
		// we don't know exactly how many they are
		ScanGroup<T, true>(nullptr, skip_count);
	}
};

template <class T>
unique_ptr<SegmentScanState> AlpInitScan(ColumnSegment &segment) {
	printf("Init Scan\n");
	auto result = make_uniq_base<SegmentScanState, AlpScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void AlpScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	printf("Scan partial\n");
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	auto &scan_state = (AlpScanState<T> &)*state.scan_state;

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	current_result_ptr += result_offset;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		const auto remaining = scan_count - scanned;
		const idx_t to_scan = MinValue(remaining, scan_state.LeftInGroup());

		scan_state.template ScanGroup<T>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
	}
}

template <class T>
void AlpSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	printf("SKIP\n");
	auto &scan_state = (AlpScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void AlpScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	printf("SCAN\n");
	AlpScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
