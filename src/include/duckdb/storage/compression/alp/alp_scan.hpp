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
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

[[noreturn]] void ThrowAlpMetadataBeforeHeader();
[[noreturn]] void ThrowAlpMetadataTableOutOfBounds();
[[noreturn]] void ThrowAlpVectorOffsetOutOfBounds();
[[noreturn]] void ThrowAlpVectorOffsetsInvalid();
[[noreturn]] void ThrowAlpExponentOutOfRange(AlpConstants::EXPONENT_TYPE exponent,
                                             AlpConstants::EXPONENT_TYPE max_exponent);
[[noreturn]] void ThrowAlpFactorOutOfRange(AlpConstants::FACTOR_TYPE factor, AlpConstants::EXPONENT_TYPE exponent);
[[noreturn]] void ThrowAlpExceptionCountOutOfRange(AlpConstants::EXCEPTIONS_COUNT_TYPE exception_count,
                                                   idx_t vector_size);
[[noreturn]] void ThrowAlpBitWidthOutOfRange(AlpConstants::BIT_WIDTH_TYPE bit_width);
[[noreturn]] void ThrowAlpExceptionPositionOutOfRange(AlpConstants::EXCEPTION_POSITION_TYPE position,
                                                      idx_t vector_size);

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
		alp::AlpDecompression<T>::Decompress(const_data_ptr_cast(for_encoded), value_buffer, count, v_factor,
		                                     v_exponent, exceptions_count, exceptions, exceptions_positions,
		                                     frame_of_reference, bit_width);
	}

public:
	idx_t index;
	T decoded_values[AlpConstants::ALP_VECTOR_SIZE];
	//! Exception values read from the segment after validating exceptions_count <= vector_size.
	T exceptions[AlpConstants::ALP_VECTOR_SIZE];
	//! Exception positions read from the segment and validated as exceptions_positions[i] < vector_size.
	AlpConstants::EXCEPTION_POSITION_TYPE exceptions_positions[AlpConstants::ALP_VECTOR_SIZE];
	//! Packed values read from the segment through a bounded vector reader.
	AlpConstants::ENCODED_VALUE_TYPE for_encoded[AlpConstants::ALP_VECTOR_SIZE];
	//! Exponent or UNCOMPRESSED_MODE_SENTINEL read from the segment.
	//! Exponents are validated as v_exponent <= AlpTypedConstants<T>::MAX_EXPONENT.
	AlpConstants::EXPONENT_TYPE v_exponent;
	//! Factor read from the segment and validated as v_factor <= v_exponent.
	AlpConstants::FACTOR_TYPE v_factor;
	//! Exception count read from the segment and validated as exceptions_count <= vector_size.
	AlpConstants::EXCEPTIONS_COUNT_TYPE exceptions_count;
	//! Frame of reference read from the segment, no validation is needed for FRAME_OF_REFERENCE_TYPE values.
	AlpConstants::FRAME_OF_REFERENCE_TYPE frame_of_reference;
	//! Bit width read from the segment and validated as bit_width <= AlpConstants::MAX_BIT_WIDTH.
	AlpConstants::BIT_WIDTH_TYPE bit_width;
};

template <class T>
struct AlpScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	using METADATA_POINTER_TYPE = AlpConstants::METADATA_POINTER_TYPE;

	struct SegmentLayout {
		//! Vector data between the segment header and reverse offset table.
		CompressionSegmentReader data;
		//! Reverse vector offsets read from the segment after the table extent was validated.
		//! Each offset is validated when its vector is loaded.
		unsafe_array_ptr<const METADATA_POINTER_TYPE> vector_offsets;
	};

	static SegmentLayout ReadSegmentLayout(const BufferHandle &handle, ColumnSegment &segment, idx_t count) {
		auto reader = CompressionSegmentReader::FromSegment(handle, segment, "ALP segment");
		auto metadata_end = reader.template Read<METADATA_POINTER_TYPE>();
		if (metadata_end < AlpConstants::HEADER_SIZE) {
			ThrowAlpMetadataBeforeHeader();
		}
		reader = reader.GetSubReader(0, metadata_end, "ALP segment");

		auto vector_count = count / AlpConstants::ALP_VECTOR_SIZE + (count % AlpConstants::ALP_VECTOR_SIZE != 0);
		if (vector_count > (metadata_end - AlpConstants::HEADER_SIZE) / AlpConstants::METADATA_POINTER_SIZE) {
			ThrowAlpMetadataTableOutOfBounds();
		}
		auto metadata_size = vector_count * AlpConstants::METADATA_POINTER_SIZE;
		auto metadata_start = metadata_end - metadata_size;
		auto data =
		    reader.GetSubReader(AlpConstants::HEADER_SIZE, metadata_start - AlpConstants::HEADER_SIZE, "ALP data");
		auto vector_offsets = reader.template GetArray<METADATA_POINTER_TYPE>(metadata_start, vector_count);
		return {data, vector_offsets};
	}

	explicit AlpScanState(BufferHandle handle_p, ColumnSegment &segment)
	    : handle(std::move(handle_p)), count(segment.count), layout(ReadSegmentLayout(handle, segment, count)) {
	}

	BufferHandle handle;
	//! Row count read from the segment, used to derive the vector count and final vector size.
	idx_t count;
	SegmentLayout layout;
	idx_t total_value_count = 0;
	AlpVectorState<T> vector_state;

	idx_t LeftInVector() const {
		return AlpConstants::ALP_VECTOR_SIZE - (total_value_count % AlpConstants::ALP_VECTOR_SIZE);
	}

	inline bool VectorFinished() const {
		return (total_value_count % AlpConstants::ALP_VECTOR_SIZE) == 0;
	}

	//! Returns a vector offset read from the bounded reverse metadata table.
	METADATA_POINTER_TYPE GetVectorOffset(idx_t vector_index) const {
		D_ASSERT(vector_index < layout.vector_offsets.size());
		return layout.vector_offsets[layout.vector_offsets.size() - 1 - vector_index];
	}

	CompressionSegmentReader GetVectorReader(idx_t vector_index) const {
		D_ASSERT(vector_index < layout.vector_offsets.size());
		auto vector_start = GetVectorOffset(vector_index);
		if (vector_start < AlpConstants::HEADER_SIZE) {
			ThrowAlpVectorOffsetOutOfBounds();
		}
		auto data_start = vector_start - AlpConstants::HEADER_SIZE;

		auto data_end = layout.data.Size();
		if (vector_index + 1 < layout.vector_offsets.size()) {
			auto vector_end = GetVectorOffset(vector_index + 1);
			if (vector_end < AlpConstants::HEADER_SIZE) {
				ThrowAlpVectorOffsetOutOfBounds();
			}
			data_end = vector_end - AlpConstants::HEADER_SIZE;
		}
		if (data_start > data_end || data_end > layout.data.Size()) {
			ThrowAlpVectorOffsetsInvalid();
		}
		return layout.data.GetSubReader(data_start, data_end - data_start, "ALP vector");
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
		D_ASSERT(total_value_count < count);
		idx_t vector_size = MinValue((idx_t)AlpConstants::ALP_VECTOR_SIZE, count - total_value_count);
		total_value_count += vector_size;
	}

	template <bool SKIP = false>
	void LoadVector(T *value_buffer) {
		vector_state.Reset();

		auto vector_index = total_value_count / AlpConstants::ALP_VECTOR_SIZE;
		auto vector_reader = GetVectorReader(vector_index);
		idx_t vector_size = MinValue((idx_t)AlpConstants::ALP_VECTOR_SIZE, (count - total_value_count));

		// Load the vector data
		vector_state.v_exponent = vector_reader.template Read<AlpConstants::EXPONENT_TYPE>();

		const bool uncompressed_mode = vector_state.v_exponent == AlpConstants::UNCOMPRESSED_MODE_SENTINEL;
		if (uncompressed_mode) {
			const idx_t value_buffer_copy_size = sizeof(T) * vector_size;
			if (!SKIP) {
				// Read uncompressed values
				vector_reader.ReadBytesInto(data_ptr_cast(value_buffer), value_buffer_copy_size);
			} else {
				vector_reader.Skip(value_buffer_copy_size);
			}
			return;
		}
		if (vector_state.v_exponent > AlpTypedConstants<T>::MAX_EXPONENT) {
			ThrowAlpExponentOutOfRange(vector_state.v_exponent, AlpTypedConstants<T>::MAX_EXPONENT);
		}
		vector_state.v_factor = vector_reader.template Read<AlpConstants::FACTOR_TYPE>();
		vector_state.exceptions_count = vector_reader.template Read<AlpConstants::EXCEPTIONS_COUNT_TYPE>();
		vector_state.frame_of_reference = vector_reader.template Read<AlpConstants::FRAME_OF_REFERENCE_TYPE>();
		vector_state.bit_width = vector_reader.template Read<AlpConstants::BIT_WIDTH_TYPE>();

		if (vector_state.exceptions_count > vector_size) {
			ThrowAlpExceptionCountOutOfRange(vector_state.exceptions_count, vector_size);
		}
		if (vector_state.v_factor > vector_state.v_exponent) {
			ThrowAlpFactorOutOfRange(vector_state.v_factor, vector_state.v_exponent);
		}
		if (vector_state.bit_width > AlpConstants::MAX_BIT_WIDTH) {
			ThrowAlpBitWidthOutOfRange(vector_state.bit_width);
		}

		if (vector_state.bit_width > 0) {
			auto bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.bit_width);
			vector_reader.ReadBytesIntoArray(vector_state.for_encoded, bp_size);
		}

		if (vector_state.exceptions_count > 0) {
			//! Load the exceptions
			vector_reader.ReadIntoArray(vector_state.exceptions, vector_state.exceptions_count);

			//! Load the exceptions_positions
			vector_reader.ReadIntoArray(vector_state.exceptions_positions, vector_state.exceptions_count);

			//! The exception positions index into the decoded vector, so they must stay within its bounds
			for (idx_t i = 0; i < vector_state.exceptions_count; i++) {
				if (vector_state.exceptions_positions[i] >= vector_size) {
					ThrowAlpExceptionPositionOutOfRange(vector_state.exceptions_positions[i], vector_size);
				}
			}
		}

		// Decode all the vector values to the specified 'value_buffer'
		vector_state.template LoadValues<SKIP>(value_buffer, vector_size);
	}

public:
	//! Skip the next 'skip_count' values, we don't store the values
	void Skip(ColumnSegment &col_segment, idx_t skip_count) {
		D_ASSERT(total_value_count <= count);
		D_ASSERT(skip_count <= count - total_value_count);
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
unique_ptr<SegmentScanState> AlpInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(context, segment.GetBlockHandle());
	auto result = make_uniq_base<SegmentScanState, AlpScanState<T>>(std::move(handle), segment);
	return result;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void AlpScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                    idx_t result_offset) {
	auto &scan_state = (AlpScanState<T> &)*state.scan_state;
	D_ASSERT(scan_state.total_value_count <= scan_state.count);
	D_ASSERT(scan_count <= scan_state.count - scan_state.total_value_count);

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetDataMutable<T>(result);
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
