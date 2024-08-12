//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alprd/alprd_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alprd/algorithm/alprd.hpp"
#include "duckdb/storage/compression/alprd/alprd_constants.hpp"

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
struct AlpRDVectorState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	void Reset() {
		index = 0;
	}

	// Scan of the data itself
	template <bool SKIP = false>
	void Scan(uint8_t *dest, idx_t count) {
		if (!SKIP) {
			memcpy(dest, (void *)(decoded_values + index), sizeof(T) * count);
		}
		index += count;
	}

	template <bool SKIP>
	void LoadValues(EXACT_TYPE *values_buffer, idx_t count) {
		if (SKIP) {
			return;
		}
		values_buffer[0] = (EXACT_TYPE)0;
		alp::AlpRDDecompression<T>::Decompress(left_encoded, right_encoded, left_parts_dict, values_buffer, count,
		                                       exceptions_count, exceptions, exceptions_positions, left_bit_width,
		                                       right_bit_width);
	}

public:
	idx_t index;
	uint8_t left_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint8_t right_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	EXACT_TYPE decoded_values[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_count;
	uint8_t right_bit_width;
	uint8_t left_bit_width;
	uint16_t left_parts_dict[AlpRDConstants::MAX_DICTIONARY_SIZE];
};

template <class T>
struct AlpRDScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	explicit AlpRDScanState(ColumnSegment &segment) : segment(segment), count(segment.count) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

		handle = buffer_manager.Pin(segment.block);
		// ScanStates never exceed the boundaries of a Segment,
		// but are not guaranteed to start at the beginning of the Block
		segment_data = handle.Ptr() + segment.GetBlockOffset();
		auto metadata_offset = Load<uint32_t>(segment_data);
		metadata_ptr = segment_data + metadata_offset;

		// Load the Right Bit Width which is in the segment header after the pointer to the first metadata
		vector_state.right_bit_width = Load<uint8_t>(segment_data + AlpRDConstants::METADATA_POINTER_SIZE);
		vector_state.left_bit_width =
		    Load<uint8_t>(segment_data + AlpRDConstants::METADATA_POINTER_SIZE + AlpRDConstants::RIGHT_BIT_WIDTH_SIZE);

		uint8_t actual_dictionary_size =
		    Load<uint8_t>(segment_data + AlpRDConstants::METADATA_POINTER_SIZE + AlpRDConstants::RIGHT_BIT_WIDTH_SIZE +
		                  AlpRDConstants::LEFT_BIT_WIDTH_SIZE);
		uint8_t actual_dictionary_size_bytes = actual_dictionary_size * AlpRDConstants::DICTIONARY_ELEMENT_SIZE;

		// Load the left parts dictionary which is after the segment header and is of a fixed size
		memcpy(vector_state.left_parts_dict, (void *)(segment_data + AlpRDConstants::HEADER_SIZE),
		       actual_dictionary_size_bytes);
	}

	BufferHandle handle;
	data_ptr_t metadata_ptr;
	data_ptr_t segment_data;
	idx_t total_value_count = 0;
	AlpRDVectorState<T> vector_state;

	ColumnSegment &segment;
	idx_t count;

	idx_t LeftInVector() const {
		return AlpRDConstants::ALP_VECTOR_SIZE - (total_value_count % AlpRDConstants::ALP_VECTOR_SIZE);
	}

	inline bool VectorFinished() const {
		return (total_value_count % AlpRDConstants::ALP_VECTOR_SIZE) == 0;
	}

	// Scan up to a vector boundary
	template <class EXACT_TYPE, bool SKIP = false>
	void ScanVector(EXACT_TYPE *values, idx_t vector_size) {
		D_ASSERT(vector_size <= AlpRDConstants::ALP_VECTOR_SIZE);
		D_ASSERT(vector_size <= LeftInVector());
		if (VectorFinished() && total_value_count < count) {
			if (vector_size == AlpRDConstants::ALP_VECTOR_SIZE) {
				LoadVector<SKIP>(values);
				total_value_count += vector_size;
				return;
			} else {
				// Even if SKIP is given, the vector size is not big enough to be able to fully skip the entire vector
				LoadVector<false>(vector_state.decoded_values);
			}
		}
		vector_state.template Scan<SKIP>((uint8_t *)values, vector_size);

		total_value_count += vector_size;
	}

	// Using the metadata, we can avoid loading any of the data if we don't care about the vector at all
	void SkipVector() {
		// Skip the offset indicating where the data starts
		metadata_ptr -= AlpRDConstants::METADATA_POINTER_SIZE;
		idx_t vector_size = MinValue((idx_t)AlpRDConstants::ALP_VECTOR_SIZE, count - total_value_count);
		total_value_count += vector_size;
	}

	template <bool SKIP = false>
	void LoadVector(EXACT_TYPE *value_buffer) {
		vector_state.Reset();

		// Load the offset (metadata) indicating where the vector data starts
		metadata_ptr -= AlpRDConstants::METADATA_POINTER_SIZE;
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		D_ASSERT(data_byte_offset < segment.GetBlockManager().GetBlockSize());

		idx_t vector_size = MinValue((idx_t)AlpRDConstants::ALP_VECTOR_SIZE, (count - total_value_count));

		data_ptr_t vector_ptr = segment_data + data_byte_offset;

		// Load the vector data
		vector_state.exceptions_count = Load<uint16_t>(vector_ptr);
		vector_ptr += AlpRDConstants::EXCEPTIONS_COUNT_SIZE;
		D_ASSERT(vector_state.exceptions_count <= vector_size);

		auto left_bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.left_bit_width);
		auto right_bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.right_bit_width);

		memcpy(vector_state.left_encoded, (void *)vector_ptr, left_bp_size);
		vector_ptr += left_bp_size;

		memcpy(vector_state.right_encoded, (void *)vector_ptr, right_bp_size);
		vector_ptr += right_bp_size;

		if (vector_state.exceptions_count > 0) {
			memcpy(vector_state.exceptions, (void *)vector_ptr,
			       AlpRDConstants::EXCEPTION_SIZE * vector_state.exceptions_count);
			vector_ptr += AlpRDConstants::EXCEPTION_SIZE * vector_state.exceptions_count;
			memcpy(vector_state.exceptions_positions, (void *)vector_ptr,
			       AlpRDConstants::EXCEPTION_POSITION_SIZE * vector_state.exceptions_count);
		}

		// Decode all the vector values to the specified 'value_buffer'
		vector_state.template LoadValues<SKIP>(value_buffer, vector_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &col_segment, idx_t skip_count) {
		if (total_value_count != 0 && !VectorFinished()) {
			// Finish skipping the current vector
			idx_t to_skip = MinValue<idx_t>(skip_count, LeftInVector());
			ScanVector<EXACT_TYPE, true>(nullptr, to_skip);
			skip_count -= to_skip;
		}
		// Figure out how many entire vectors we can skip
		// For these vectors, we don't even need to process the metadata or values
		idx_t vectors_to_skip = skip_count / AlpRDConstants::ALP_VECTOR_SIZE;
		for (idx_t i = 0; i < vectors_to_skip; i++) {
			SkipVector();
		}
		skip_count -= AlpRDConstants::ALP_VECTOR_SIZE * vectors_to_skip;
		if (skip_count == 0) {
			return;
		}
		// For the last vector that this skip (partially) touches, we do need to
		// load the metadata and values into the vector_state because
		// we don't know exactly how many they are
		ScanVector<EXACT_TYPE, true>(nullptr, skip_count);
	}
};

template <class T>
unique_ptr<SegmentScanState> AlpRDInitScan(ColumnSegment &segment) {
	auto result = make_uniq_base<SegmentScanState, AlpRDScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void AlpRDScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                      idx_t result_offset) {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	auto &scan_state = (AlpRDScanState<T> &)*state.scan_state;

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetData<EXACT_TYPE>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	current_result_ptr += result_offset;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		const auto remaining = scan_count - scanned;
		const idx_t to_scan = MinValue(remaining, scan_state.LeftInVector());

		scan_state.template ScanVector<EXACT_TYPE>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
	}
}

template <class T>
void AlpRDSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (AlpRDScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void AlpRDScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	AlpRDScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
