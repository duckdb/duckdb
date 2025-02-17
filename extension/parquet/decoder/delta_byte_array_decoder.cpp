#include "decoder/delta_byte_array_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

DeltaByteArrayDecoder::DeltaByteArrayDecoder(ColumnReader &reader) : reader(reader) {
}

void DeltaByteArrayDecoder::ReadDbpData(Allocator &allocator, ResizeableBuffer &buffer, ResizeableBuffer &result_buffer,
                                        idx_t &value_count) {
	auto decoder = make_uniq<DbpDecoder>(buffer.ptr, buffer.len);
	value_count = decoder->TotalValues();
	result_buffer.reset();
	result_buffer.resize(allocator, sizeof(uint32_t) * value_count);
	decoder->GetBatch<uint32_t>(result_buffer.ptr, value_count);
	decoder->Finalize();
	buffer.inc(buffer.len - decoder->BufferPtr().len);
}

void DeltaByteArrayDecoder::InitializePage() {
	if (reader.Type().InternalType() != PhysicalType::VARCHAR) {
		throw std::runtime_error("Delta Byte Array encoding is only supported for string/blob data");
	}
	auto &block = *reader.block;
	auto &allocator = reader.reader.allocator;
	idx_t prefix_count, suffix_count;
	auto &prefix_buffer = reader.encoding_buffers[0];
	auto &suffix_buffer = reader.encoding_buffers[1];
	ReadDbpData(allocator, block, prefix_buffer, prefix_count);
	ReadDbpData(allocator, block, suffix_buffer, suffix_count);
	if (prefix_count != suffix_count) {
		throw std::runtime_error("DELTA_BYTE_ARRAY - prefix and suffix counts are different - corrupt file?");
	}
	if (prefix_count == 0) {
		// no values
		byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
		return;
	}
	auto prefix_data = reinterpret_cast<uint32_t *>(prefix_buffer.ptr);
	auto suffix_data = reinterpret_cast<uint32_t *>(suffix_buffer.ptr);
	byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, prefix_count);
	byte_array_count = prefix_count;
	delta_offset = 0;
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t i = 0; i < prefix_count; i++) {
		auto str_len = prefix_data[i] + suffix_data[i];
		block.available(suffix_data[i]);
		string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
		auto result_data = string_data[i].GetDataWriteable();
		if (prefix_data[i] > 0) {
			if (i == 0 || prefix_data[i] > string_data[i - 1].GetSize()) {
				throw std::runtime_error("DELTA_BYTE_ARRAY - prefix is out of range - corrupt file?");
			}
			memcpy(result_data, string_data[i - 1].GetData(), prefix_data[i]);
		}
		memcpy(result_data + prefix_data[i], block.ptr, suffix_data[i]);
		block.inc(suffix_data[i]);
		string_data[i].Finalize();
	}
}

void DeltaByteArrayDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	if (!byte_array_data) {
		throw std::runtime_error("Internal error - DeltaByteArray called but there was no byte_array_data set");
	}
	auto result_ptr = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t row_idx = 0; row_idx < read_count; row_idx++) {
		if (defines && defines[row_idx + result_offset] != reader.MaxDefine()) {
			result_mask.SetInvalid(row_idx + result_offset);
			continue;
		}
		if (delta_offset >= byte_array_count) {
			throw IOException("DELTA_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
			                  "read of %d from %d entries) - corrupt file?",
			                  delta_offset + 1, byte_array_count);
		}
		result_ptr[row_idx + result_offset] = string_data[delta_offset++];
	}
	StringVector::AddHeapReference(result, *byte_array_data);
}

void DeltaByteArrayDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	if (!byte_array_data) {
		throw std::runtime_error("Internal error - DeltaByteArray called but there was no byte_array_data set");
	}
	for (idx_t row_idx = 0; row_idx < skip_count; row_idx++) {
		if (defines && defines[row_idx] != reader.MaxDefine()) {
			continue;
		}
		if (delta_offset >= byte_array_count) {
			throw IOException("DELTA_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
			                  "read of %d from %d entries) - corrupt file?",
			                  delta_offset + 1, byte_array_count);
		}
		delta_offset++;
	}
}

} // namespace duckdb
