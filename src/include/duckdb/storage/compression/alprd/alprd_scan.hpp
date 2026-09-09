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

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

[[noreturn]] void ThrowAlpRDMetadataBeforeHeader();
[[noreturn]] void ThrowAlpRDRightBitWidthOutOfRange(AlpRDConstants::BIT_WIDTH_TYPE bit_width,
                                                    AlpRDConstants::BIT_WIDTH_TYPE max_bit_width);
[[noreturn]] void ThrowAlpRDLeftBitWidthOutOfRange(AlpRDConstants::BIT_WIDTH_TYPE bit_width);
[[noreturn]] void ThrowAlpRDDictionarySizeExceedsMaximum();
[[noreturn]] void ThrowAlpRDMetadataTableOutOfBounds();
[[noreturn]] void ThrowAlpRDVectorOffsetOutOfBounds();
[[noreturn]] void ThrowAlpRDVectorOffsetsInvalid();
[[noreturn]] void ThrowAlpRDExceptionCountOutOfRange(AlpRDConstants::EXCEPTIONS_COUNT_TYPE exception_count,
                                                     idx_t vector_size);
[[noreturn]] void ThrowAlpRDExceptionPositionOutOfRange(AlpRDConstants::EXCEPTION_POSITION_TYPE position,
                                                        idx_t vector_size);

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
		alp::AlpRDDecompression<T>::Decompress(const_data_ptr_cast(left_encoded), const_data_ptr_cast(right_encoded),
		                                       left_parts_dict, values_buffer, count, exceptions_count, exceptions,
		                                       exceptions_positions, left_bit_width, right_bit_width);
	}

public:
	idx_t index;
	//! The element types below provide maximum packed capacity and alignment for bit unpacking.
	AlpRDConstants::DICTIONARY_ELEMENT_TYPE left_encoded[AlpRDConstants::ALP_VECTOR_SIZE];
	EXACT_TYPE right_encoded[AlpRDConstants::ALP_VECTOR_SIZE];

	EXACT_TYPE decoded_values[AlpRDConstants::ALP_VECTOR_SIZE];
	//! Exception values read from the segment after validating exceptions_count <= vector_size.
	AlpRDConstants::EXCEPTION_TYPE exceptions[AlpRDConstants::ALP_VECTOR_SIZE];
	//! Exception positions read from the segment and validated as exceptions_positions[i] < vector_size.
	AlpRDConstants::EXCEPTION_POSITION_TYPE exceptions_positions[AlpRDConstants::ALP_VECTOR_SIZE];
	//! Exception count or UNCOMPRESSED_MODE_SENTINEL read from the segment.
	//! Compressed-vector counts are validated as exceptions_count <= vector_size.
	AlpRDConstants::EXCEPTIONS_COUNT_TYPE exceptions_count;
	//! Right bit width read from the segment, validated as right_bit_width <= MAX_RIGHT_BIT_WIDTH.
	AlpRDConstants::BIT_WIDTH_TYPE right_bit_width;
	//! Left bit width read from the segment, validated as left_bit_width <= AlpRDConstants::MAX_DICTIONARY_BIT_WIDTH.
	AlpRDConstants::BIT_WIDTH_TYPE left_bit_width;
	//! Dictionary read from the segment, entries beyond dictionary_count are zeroed.
	AlpRDConstants::DICTIONARY_ELEMENT_TYPE left_parts_dict[AlpRDConstants::MAX_DICTIONARY_SIZE];
};

template <class T>
struct AlpRDScanState : public SegmentScanState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	//! Reconstruction shifts a left part by this width, so it must be smaller than the reconstructed value.
	static constexpr AlpRDConstants::BIT_WIDTH_TYPE MAX_RIGHT_BIT_WIDTH =
	    static_cast<AlpRDConstants::BIT_WIDTH_TYPE>(sizeof(EXACT_TYPE) * 8 - 1);

	struct SegmentLayout {
		//! Vector data between the dictionary and reverse offset table.
		CompressionSegmentReader data;
		//! Reverse vector offsets read from the segment after the table extent was validated.
		//! Each offset is validated when its vector is loaded.
		unsafe_array_ptr<const AlpRDConstants::METADATA_POINTER_TYPE> vector_offsets;
		//! Byte offset derived after the dictionary byte span was validated.
		idx_t data_start;
	};

	static SegmentLayout ReadSegmentLayout(const BufferHandle &handle, ColumnSegment &segment, idx_t count,
	                                       AlpRDVectorState<T> &vector_state) {
		auto reader = CompressionSegmentReader::FromSegment(handle, segment, "ALPRD segment");
		auto metadata_end = reader.template Read<AlpRDConstants::METADATA_POINTER_TYPE>();
		if (metadata_end < AlpRDConstants::HEADER_SIZE) {
			ThrowAlpRDMetadataBeforeHeader();
		}
		reader = reader.GetSubReader(0, metadata_end, "ALPRD segment");
		reader.Skip(AlpRDConstants::METADATA_POINTER_SIZE);

		vector_state.right_bit_width = reader.template Read<AlpRDConstants::BIT_WIDTH_TYPE>();
		if (vector_state.right_bit_width > MAX_RIGHT_BIT_WIDTH) {
			ThrowAlpRDRightBitWidthOutOfRange(vector_state.right_bit_width, MAX_RIGHT_BIT_WIDTH);
		}

		vector_state.left_bit_width = reader.template Read<AlpRDConstants::BIT_WIDTH_TYPE>();
		if (vector_state.left_bit_width > AlpRDConstants::MAX_DICTIONARY_BIT_WIDTH) {
			ThrowAlpRDLeftBitWidthOutOfRange(vector_state.left_bit_width);
		}

		auto dictionary_count = reader.template Read<AlpRDConstants::DICTIONARY_COUNT_TYPE>();
		if (dictionary_count > AlpRDConstants::MAX_DICTIONARY_SIZE) {
			ThrowAlpRDDictionarySizeExceedsMaximum();
		}

		reader.ReadIntoArray(vector_state.left_parts_dict, dictionary_count);
		// Exception values use a dictionary index beyond the stored entries, so the remaining entries must be zero.
		memset(vector_state.left_parts_dict + dictionary_count, 0,
		       (AlpRDConstants::MAX_DICTIONARY_SIZE - dictionary_count) * AlpRDConstants::DICTIONARY_ELEMENT_SIZE);

		auto data_start = reader.Position();
		auto vector_count = count / AlpRDConstants::ALP_VECTOR_SIZE + (count % AlpRDConstants::ALP_VECTOR_SIZE != 0);
		if (vector_count > (metadata_end - data_start) / AlpRDConstants::METADATA_POINTER_SIZE) {
			ThrowAlpRDMetadataTableOutOfBounds();
		}
		auto metadata_size = vector_count * AlpRDConstants::METADATA_POINTER_SIZE;
		auto metadata_start = metadata_end - metadata_size;
		auto data = reader.GetSubReader(data_start, metadata_start - data_start, "ALPRD data");
		auto vector_offsets =
		    reader.template GetArray<AlpRDConstants::METADATA_POINTER_TYPE>(metadata_start, vector_count);
		return {data, vector_offsets, data_start};
	}

	explicit AlpRDScanState(BufferHandle handle_p, ColumnSegment &segment)
	    : handle(std::move(handle_p)), count(segment.count),
	      layout(ReadSegmentLayout(handle, segment, count, vector_state)) {
	}

	BufferHandle handle;
	//! Row count read from the segment, used to derive the vector count and final vector size.
	idx_t count;
	AlpRDVectorState<T> vector_state;
	SegmentLayout layout;
	idx_t total_value_count = 0;

	//! Returns a vector offset read from the bounded reverse metadata table.
	AlpRDConstants::METADATA_POINTER_TYPE GetVectorOffset(idx_t vector_index) const {
		D_ASSERT(vector_index < layout.vector_offsets.size());
		return layout.vector_offsets[layout.vector_offsets.size() - 1 - vector_index];
	}

	CompressionSegmentReader GetVectorReader(idx_t vector_index) const {
		D_ASSERT(vector_index < layout.vector_offsets.size());
		auto vector_start = GetVectorOffset(vector_index);
		if (vector_start < layout.data_start) {
			ThrowAlpRDVectorOffsetOutOfBounds();
		}
		auto data_start = vector_start - layout.data_start;

		auto data_end = layout.data.Size();
		if (vector_index + 1 < layout.vector_offsets.size()) {
			auto vector_end = GetVectorOffset(vector_index + 1);
			if (vector_end < layout.data_start) {
				ThrowAlpRDVectorOffsetOutOfBounds();
			}
			data_end = vector_end - layout.data_start;
		}
		if (data_start > data_end || data_end > layout.data.Size()) {
			ThrowAlpRDVectorOffsetsInvalid();
		}
		return layout.data.GetSubReader(data_start, data_end - data_start, "ALPRD vector");
	}

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
		D_ASSERT(total_value_count < count);
		idx_t vector_size = MinValue((idx_t)AlpRDConstants::ALP_VECTOR_SIZE, count - total_value_count);
		total_value_count += vector_size;
	}

	template <bool SKIP = false>
	void LoadVector(EXACT_TYPE *value_buffer) {
		vector_state.Reset();

		auto vector_index = total_value_count / AlpRDConstants::ALP_VECTOR_SIZE;
		auto vector_reader = GetVectorReader(vector_index);
		idx_t vector_size = MinValue((idx_t)AlpRDConstants::ALP_VECTOR_SIZE, (count - total_value_count));

		// Load the vector data
		vector_state.exceptions_count = vector_reader.template Read<AlpRDConstants::EXCEPTIONS_COUNT_TYPE>();

		const bool uncompressed_mode = vector_state.exceptions_count == AlpRDConstants::UNCOMPRESSED_MODE_SENTINEL;
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

		if (vector_state.exceptions_count > vector_size) {
			ThrowAlpRDExceptionCountOutOfRange(vector_state.exceptions_count, vector_size);
		}

		auto left_bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.left_bit_width);
		auto right_bp_size = BitpackingPrimitives::GetRequiredSize(vector_size, vector_state.right_bit_width);
		vector_reader.ReadBytesIntoArray(vector_state.left_encoded, left_bp_size);
		vector_reader.ReadBytesIntoArray(vector_state.right_encoded, right_bp_size);

		if (vector_state.exceptions_count > 0) {
			//! Load the exceptions
			vector_reader.ReadIntoArray(vector_state.exceptions, vector_state.exceptions_count);

			//! Load the exceptions_positions
			vector_reader.ReadIntoArray(vector_state.exceptions_positions, vector_state.exceptions_count);

			//! The exception positions index into the decoded vector, so they must stay within its bounds
			for (idx_t i = 0; i < vector_state.exceptions_count; i++) {
				if (vector_state.exceptions_positions[i] >= vector_size) {
					ThrowAlpRDExceptionPositionOutOfRange(vector_state.exceptions_positions[i], vector_size);
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
unique_ptr<SegmentScanState> AlpRDInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(context, segment.GetBlockHandle());
	auto result = make_uniq_base<SegmentScanState, AlpRDScanState<T>>(std::move(handle), segment);
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
	D_ASSERT(scan_state.total_value_count <= scan_state.count);
	D_ASSERT(scan_count <= scan_state.count - scan_state.total_value_count);

	// Get the pointer to the result values
	auto current_result_ptr = FlatVector::GetDataMutableUnsafe<EXACT_TYPE>(result);
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
