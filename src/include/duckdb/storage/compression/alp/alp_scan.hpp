//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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
struct AlpVectorState {
public:
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
	void LoadValues(T *value_buffer, idx_t count) {
		if (SKIP) {
			return;
		}
		value_buffer[0] = (T)0;
		alp::AlpDecompression<T>::Decompress(for_encoded, value_buffer, count, v_factor, v_exponent, exceptions_count,
		                                     exceptions, exceptions_positions, frame_of_reference, bit_width);
	}

public:
	idx_t index;
	T decoded_values[AlpConstants::ALP_VECTOR_SIZE];
	T exceptions[AlpConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpConstants::ALP_VECTOR_SIZE];
	uint8_t for_encoded[AlpConstants::ALP_VECTOR_SIZE * 8];
	uint8_t v_exponent;
	uint8_t v_factor;
	uint16_t exceptions_count;
	uint64_t frame_of_reference;
	uint8_t bit_width;
};

template <class T>
struct AlpScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

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
	AlpVectorState<T> vector_state;

	ColumnSegment &segment;
	idx_t count;

	idx_t LeftInVector() const {
		return AlpConstants::ALP_VECTOR_SIZE - (total_value_count % AlpConstants::ALP_VECTOR_SIZE);
	}

	inline bool VectorFinished() const {
		return (total_value_count % AlpConstants::ALP_VECTOR_SIZE) == 0;
	}

	// Scan up to a vector boundary
	template <class EXACT_TYPE, bool SKIP = false>
	void ScanVector(T *values, idx_t vector_size) {
		D_ASSERT(vector_size <= AlpConstants::ALP_VECTOR_SIZE);
		D_ASSERT(vector_size <= LeftInVector());
		if (VectorFinished() && total_value_count < count) {
			if (vector_size == AlpConstants::ALP_VECTOR_SIZE) {
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
		metadata_ptr -= AlpConstants::METADATA_POINTER_SIZE;
		idx_t vector_size = MinValue((idx_t)AlpConstants::ALP_VECTOR_SIZE, count - total_value_count);
		total_value_count += vector_size;
	}

	template <bool SKIP = false>
	void LoadVector(T *value_buffer) {
		vector_state.Reset();

		// Load the offset (metadata) indicating where the vector data starts
		metadata_ptr -= AlpConstants::METADATA_POINTER_SIZE;
		auto data_byte_offset = Load<uint32_t>(metadata_ptr);
		D_ASSERT(data_byte_offset < segment.GetBlockManager().GetBlockSize());

		idx_t vector_size = MinValue((idx_t)AlpConstants::ALP_VECTOR_SIZE, (count - total_value_count));

		data_ptr_t vector_ptr = segment_data + data_byte_offset;

		// Load the vector data
		vector_state.v_exponent = Load<uint8_t>(vector_ptr);
		vector_ptr += AlpConstants::EXPONENT_SIZE;

		vector_state.v_factor = Load<uint8_t>(vector_ptr);
		vector_ptr += AlpConstants::FACTOR_SIZE;

		vector_state.exceptions_count = Load<uint16_t>(vector_ptr);
		vector_ptr += AlpConstants::EXCEPTIONS_COUNT_SIZE;

		vector_state.frame_of_reference = Load<uint64_t>(vector_ptr);
		vector_ptr += AlpConstants::FOR_SIZE;

		vector_state.bit_width = Load<uint8_t>(vector_ptr);
		vector_ptr += AlpConstants::BIT_WIDTH_SIZE;

		D_ASSERT(vector_state.exceptions_count <= vector_size);
		D_ASSERT(vector_state.v_exponent <= AlpTypedConstants<T>::MAX_EXPONENT);
		D_ASSERT(vector_state.v_factor <= vector_state.v_exponent);
		D_ASSERT(vector_state.bit_width <= sizeof(uint64_t) * 8);

		if (vector_state.bit_width > 0) {
			auto bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.bit_width);
			memcpy(vector_state.for_encoded, (void *)vector_ptr, bp_size);
			vector_ptr += bp_size;
		}

		if (vector_state.exceptions_count > 0) {
			memcpy(vector_state.exceptions, (void *)vector_ptr, sizeof(EXACT_TYPE) * vector_state.exceptions_count);
			vector_ptr += sizeof(EXACT_TYPE) * vector_state.exceptions_count;
			memcpy(vector_state.exceptions_positions, (void *)vector_ptr,
			       AlpConstants::EXCEPTION_POSITION_SIZE * vector_state.exceptions_count);
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
			ScanVector<T, true>(nullptr, to_skip);
			skip_count -= to_skip;
		}
		// Figure out how many entire vectors we can skip
		// For these vectors, we don't even need to process the metadata or values
		idx_t vectors_to_skip = skip_count / AlpConstants::ALP_VECTOR_SIZE;
		for (idx_t i = 0; i < vectors_to_skip; i++) {
			SkipVector();
		}
		skip_count -= AlpConstants::ALP_VECTOR_SIZE * vectors_to_skip;
		if (skip_count == 0) {
			return;
		}
		// For the last vector that this skip (partially) touches, we do need to
		// load the metadata and values into the vector_state because
		// we don't know exactly how many they are
		ScanVector<T, true>(nullptr, skip_count);
	}
};

template <class T>
unique_ptr<SegmentScanState> AlpInitScan(ColumnSegment &segment) {
	auto result = make_uniq_base<SegmentScanState, AlpScanState<T>>(segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void AlpScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                    idx_t result_offset) {
	auto &scan_state = (AlpScanState<T> &)*state.scan_state;

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	current_result_ptr += result_offset;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		const auto remaining = scan_count - scanned;
		const idx_t to_scan = MinValue(remaining, scan_state.LeftInVector());

		scan_state.template ScanVector<T>(current_result_ptr + scanned, to_scan);
		scanned += to_scan;
	}
}

template <class T>
void AlpSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (AlpScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

template <class T>
void AlpScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	AlpScanPartial<T>(segment, state, scan_count, result, 0);
}

} // namespace duckdb
