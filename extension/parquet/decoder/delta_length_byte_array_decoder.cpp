#include "decoder/delta_length_byte_array_decoder.hpp"
#include "decoder/delta_byte_array_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

DeltaLengthByteArrayDecoder::DeltaLengthByteArrayDecoder(ColumnReader &reader)
    : reader(reader), length_buffer(reader.encoding_buffers[0]) {
}

void DeltaLengthByteArrayDecoder::InitializePage() {
	if (reader.Type().InternalType() != PhysicalType::VARCHAR) {
		throw std::runtime_error("Delta Length Byte Array encoding is only supported for string/blob data");
	}
	// read the binary packed lengths
	auto &block = *reader.block;
	auto &allocator = reader.reader.allocator;
	DeltaByteArrayDecoder::ReadDbpData(allocator, block, length_buffer, byte_array_count);
	length_idx = 0;
}

void DeltaLengthByteArrayDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	auto &block = *reader.block;
	auto length_data = reinterpret_cast<uint32_t *>(length_buffer.ptr);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t row_idx = 0; row_idx < read_count; row_idx++) {
		auto result_idx = result_offset + row_idx;
		if (defines && defines[result_idx] != reader.MaxDefine()) {
			result_mask.SetInvalid(result_idx);
			continue;
		}
		if (length_idx >= byte_array_count) {
			throw IOException(
			    "DELTA_LENGTH_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
			    "read of %d from %d entries) - corrupt file?",
			    length_idx, byte_array_count);
		}
		auto str_len = length_data[length_idx++];
		block.available(str_len);
		result_data[result_idx] = StringVector::EmptyString(result, str_len);
		auto str_data = result_data[result_idx].GetDataWriteable();
		memcpy(str_data, block.ptr, str_len);
		block.inc(str_len);
		result_data[result_idx].Finalize();
	}
}

void DeltaLengthByteArrayDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	auto &block = *reader.block;
	auto length_data = reinterpret_cast<uint32_t *>(length_buffer.ptr);
	for (idx_t row_idx = 0; row_idx < skip_count; row_idx++) {
		if (defines && defines[row_idx] != reader.MaxDefine()) {
			continue;
		}
		if (length_idx >= byte_array_count) {
			throw IOException(
			    "DELTA_LENGTH_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
			    "read of %d from %d entries) - corrupt file?",
			    length_idx, byte_array_count);
		}
		auto str_len = length_data[length_idx++];
		block.inc(str_len);
	}
}

} // namespace duckdb
