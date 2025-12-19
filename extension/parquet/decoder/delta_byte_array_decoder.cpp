#include "decoder/delta_byte_array_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

DeltaByteArrayDecoder::DeltaByteArrayDecoder(ColumnReader &reader)
    : reader(reader), fixed_byte_buffer(), temp_buffer() {
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
	auto &schema = reader.Schema();
	bool is_fixed_len = (schema.parquet_type == duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY);

	// Only allow VARCHAR (strings/blobs) or FIXED_LEN_BYTE_ARRAY types
	if (!is_fixed_len && reader.Type().InternalType() != PhysicalType::VARCHAR) {
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

	auto prefix_data = reinterpret_cast<uint32_t *>(prefix_buffer.ptr);
	auto suffix_data = reinterpret_cast<uint32_t *>(suffix_buffer.ptr);

	if (is_fixed_len) {
		InitializeFixedLength(prefix_count, prefix_data, suffix_data, schema.type_length);
	} else {
		InitializeVariableLength(prefix_count, prefix_data, suffix_data);
	}
}

void DeltaByteArrayDecoder::InitializeVariableLength(idx_t value_count, uint32_t *prefix_data, uint32_t *suffix_data) {
	auto &block = *reader.block;

	if (value_count == 0) {
		// no values
		byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
		return;
	}

	byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, value_count);
	byte_array_count = value_count;
	delta_offset = 0;
	is_fixed_length_mode = false;

	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t i = 0; i < value_count; i++) {
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

void DeltaByteArrayDecoder::InitializeFixedLength(idx_t value_count, uint32_t *prefix_data, uint32_t *suffix_data,
                                                  idx_t type_length) {
	auto &block = *reader.block;
	auto &allocator = reader.reader.allocator;

	byte_array_count = value_count;
	delta_offset = 0;
	is_fixed_length_mode = true;
	fixed_length = type_length;

	if (value_count == 0) {
		fixed_byte_buffer.reset();
		return;
	}

	// Allocate buffer for all fixed-length values
	fixed_byte_buffer.reset();
	fixed_byte_buffer.resize(allocator, type_length * value_count);

	// Temporary buffer for reconstructing each value (stores previous value)
	unsafe_vector<uint8_t> prev_value(type_length, 0);

	auto output_ptr = fixed_byte_buffer.ptr;
	for (idx_t i = 0; i < value_count; i++) {
		auto prefix_len = prefix_data[i];
		auto suffix_len = suffix_data[i];

		// Validate: prefix + suffix must equal fixed_length
		if (prefix_len + suffix_len != type_length) {
			throw std::runtime_error(
			    "DELTA_BYTE_ARRAY on FIXED_LEN_BYTE_ARRAY: decoded length mismatch - corrupt file?");
		}

		// Copy prefix from previous value
		if (prefix_len > 0) {
			memcpy(output_ptr, prev_value.data(), prefix_len);
		}

		// Copy suffix from block
		block.available(suffix_len);
		memcpy(output_ptr + prefix_len, block.ptr, suffix_len);
		block.inc(suffix_len);

		// Save current value for next iteration's prefix
		memcpy(prev_value.data(), output_ptr, type_length);

		output_ptr += type_length;
	}
}

void DeltaByteArrayDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	if (is_fixed_length_mode) {
		ReadFixedLength(defines, read_count, result, result_offset);
	} else {
		ReadVariableLength(defines, read_count, result, result_offset);
	}
}

void DeltaByteArrayDecoder::ReadVariableLength(uint8_t *defines, idx_t read_count, Vector &result,
                                               idx_t result_offset) {
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

void DeltaByteArrayDecoder::ReadFixedLength(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	auto &allocator = reader.reader.allocator;

	// Count valid (non-NULL) values
	idx_t valid_count = reader.GetValidCount(defines, read_count, result_offset);

	if (valid_count == 0) {
		// All NULL values - just set validity mask
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t row_idx = 0; row_idx < read_count; row_idx++) {
			result_mask.SetInvalid(row_idx + result_offset);
		}
		return;
	}

	// Check we have enough decoded values
	if (delta_offset + valid_count > byte_array_count) {
		throw IOException("DELTA_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
		                  "read of %d from %d entries) - corrupt file?",
		                  delta_offset + valid_count, byte_array_count);
	}

	// Create a contiguous buffer of just the valid values for Plain() to consume
	temp_buffer.reset();
	temp_buffer.resize(allocator, fixed_length * valid_count);

	auto src_ptr = fixed_byte_buffer.ptr + (delta_offset * fixed_length);
	auto dst_ptr = temp_buffer.ptr;

	auto &result_mask = FlatVector::Validity(result);
	for (idx_t row_idx = 0; row_idx < read_count; row_idx++) {
		if (defines && defines[row_idx + result_offset] != reader.MaxDefine()) {
			result_mask.SetInvalid(row_idx + result_offset);
			continue;
		}
		// Copy this value to temp buffer
		memcpy(dst_ptr, src_ptr, fixed_length);
		dst_ptr += fixed_length;
		src_ptr += fixed_length;
		delta_offset++;
	}

	// Use Plain() for type conversion (UUID, Decimal, etc.)
	// Plain() handles the type-specific conversion (e.g., UUIDColumnReader uses UUIDValueConversion::PlainRead())
	reader.Plain(temp_buffer, defines, read_count, result_offset, result);
}

void DeltaByteArrayDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	// Skip logic is the same for both modes - just increment delta_offset for valid values
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
