#include "decoder/delta_length_byte_array_decoder.hpp"
#include "decoder/delta_byte_array_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "reader/string_column_reader.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

DeltaLengthByteArrayDecoder::DeltaLengthByteArrayDecoder(ColumnReader &reader)
    : reader(reader), length_buffer(reader.encoding_buffers[0]), length_idx(0) {
}

void DeltaLengthByteArrayDecoder::InitializePage() {
	if (reader.Type().InternalType() != PhysicalType::VARCHAR) {
		throw std::runtime_error("Delta Length Byte Array encoding is only supported for string/blob data");
	}
	// read the binary packed lengths
	auto &block = *reader.block;
	auto &allocator = reader.reader.allocator;
	DeltaByteArrayDecoder::ReadDbpData(allocator, block, length_buffer, byte_array_count);

	// Verify that the sum of DBP string lengths match up with the available string data
	idx_t total_string_length = 0;
	const auto length_data = reinterpret_cast<uint32_t *>(length_buffer.ptr);
	for (idx_t i = 0; i < byte_array_count; i++) {
		total_string_length += length_data[i];
	}
	block.available(total_string_length);

	length_idx = 0;
}

void DeltaLengthByteArrayDecoder::Read(shared_ptr<ResizeableBuffer> &block_ref, uint8_t *defines, idx_t read_count,
                                       Vector &result, idx_t result_offset) {
	if (defines) {
		ReadInternal<true>(block_ref, defines, read_count, result, result_offset);
	} else {
		ReadInternal<false>(block_ref, defines, read_count, result, result_offset);
	}
}

template <bool HAS_DEFINES>
void DeltaLengthByteArrayDecoder::ReadInternal(shared_ptr<ResizeableBuffer> &block_ref, uint8_t *const defines,
                                               const idx_t read_count, Vector &result, const idx_t result_offset) {
	auto &block = *block_ref;
	const auto length_data = reinterpret_cast<uint32_t *>(length_buffer.ptr);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);

	if (!HAS_DEFINES) {
		// Fast path: take this out of the loop below
		if (length_idx + read_count > byte_array_count) {
			throw IOException(
			    "DELTA_LENGTH_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
			    "read of %d from %d entries) - corrupt file?",
			    length_idx + read_count, byte_array_count);
		}
	}

	const auto start_ptr = block.ptr;
	for (idx_t row_idx = 0; row_idx < read_count; row_idx++) {
		const auto result_idx = result_offset + row_idx;
		if (HAS_DEFINES) {
			if (defines[result_idx] != reader.MaxDefine()) {
				result_mask.SetInvalid(result_idx);
				continue;
			}
			if (length_idx >= byte_array_count) {
				throw IOException(
				    "DELTA_LENGTH_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
				    "read of %d from %d entries) - corrupt file?",
				    length_idx, byte_array_count);
			}
		}
		const auto &str_len = length_data[length_idx++];
		result_data[result_idx] = string_t(char_ptr_cast(block.ptr), str_len);
		block.unsafe_inc(str_len);
	}

	// Verify that the strings we read are valid UTF-8
	reader.Cast<StringColumnReader>().VerifyString(char_ptr_cast(start_ptr), block.ptr - start_ptr);

	StringColumnReader::ReferenceBlock(result, block_ref);
}

void DeltaLengthByteArrayDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	if (defines) {
		SkipInternal<true>(defines, skip_count);
	} else {
		SkipInternal<false>(defines, skip_count);
	}
}

template <bool HAS_DEFINES>
void DeltaLengthByteArrayDecoder::SkipInternal(uint8_t *defines, idx_t skip_count) {
	auto &block = *reader.block;
	const auto length_data = reinterpret_cast<uint32_t *>(length_buffer.ptr);

	if (!HAS_DEFINES) {
		// Fast path: take this out of the loop below
		if (length_idx + skip_count > byte_array_count) {
			throw IOException(
			    "DELTA_LENGTH_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
			    "read of %d from %d entries) - corrupt file?",
			    length_idx + skip_count, byte_array_count);
		}
	}

	idx_t skip_bytes = 0;
	for (idx_t row_idx = 0; row_idx < skip_count; row_idx++) {
		if (HAS_DEFINES) {
			if (defines[row_idx] != reader.MaxDefine()) {
				continue;
			}
			if (length_idx >= byte_array_count) {
				throw IOException(
				    "DELTA_LENGTH_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
				    "read of %d from %d entries) - corrupt file?",
				    length_idx, byte_array_count);
			}
		}
		skip_bytes += length_data[length_idx++];
	}
	block.inc(skip_bytes);
}

} // namespace duckdb
