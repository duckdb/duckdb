#include "string_column_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// String Column Reader
//===--------------------------------------------------------------------===//
StringColumnReader::StringColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                       idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p)
    : TemplatedColumnReader<string_t, StringParquetValueConversion>(reader, std::move(type_p), schema_p, schema_idx_p,
                                                                    max_define_p, max_repeat_p) {
	fixed_width_string_length = 0;
	if (schema_p.type == Type::FIXED_LEN_BYTE_ARRAY) {
		D_ASSERT(schema_p.__isset.type_length);
		fixed_width_string_length = schema_p.type_length;
	}
}

uint32_t StringColumnReader::VerifyString(const char *str_data, uint32_t str_len, const bool is_varchar) {
	if (!is_varchar) {
		return str_len;
	}
	// verify if a string is actually UTF8, and if there are no null bytes in the middle of the string
	// technically Parquet should guarantee this, but reality is often disappointing
	UnicodeInvalidReason reason;
	size_t pos;
	auto utf_type = Utf8Proc::Analyze(str_data, str_len, &reason, &pos);
	if (utf_type == UnicodeType::INVALID) {
		throw InvalidInputException("Invalid string encoding found in Parquet file: value \"" +
		                            Blob::ToString(string_t(str_data, str_len)) + "\" is not valid UTF8!");
	}
	return str_len;
}

uint32_t StringColumnReader::VerifyString(const char *str_data, uint32_t str_len) {
	return VerifyString(str_data, str_len, Type() == LogicalTypeId::VARCHAR);
}

static shared_ptr<ResizeableBuffer> ReadDbpData(Allocator &allocator, ResizeableBuffer &buffer, idx_t &value_count) {
	auto decoder = make_uniq<DbpDecoder>(buffer.ptr, buffer.len);
	value_count = decoder->TotalValues();
	auto result = make_shared_ptr<ResizeableBuffer>();
	result->resize(allocator, sizeof(uint32_t) * value_count);
	decoder->GetBatch<uint32_t>(result->ptr, value_count);
	decoder->Finalize();
	buffer.inc(buffer.len - decoder->BufferPtr().len);
	return result;
}

void StringColumnReader::PrepareDeltaLengthByteArray(ResizeableBuffer &buffer) {
	idx_t value_count;
	auto length_buffer = ReadDbpData(reader.allocator, buffer, value_count);
	if (value_count == 0) {
		// no values
		byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
		return;
	}
	auto length_data = reinterpret_cast<uint32_t *>(length_buffer->ptr);
	byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, value_count);
	byte_array_count = value_count;
	delta_offset = 0;
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t i = 0; i < value_count; i++) {
		auto str_len = length_data[i];
		buffer.available(str_len);
		string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
		auto result_data = string_data[i].GetDataWriteable();
		memcpy(result_data, buffer.ptr, length_data[i]);
		buffer.inc(length_data[i]);
		string_data[i].Finalize();
	}
}

void StringColumnReader::PrepareDeltaByteArray(ResizeableBuffer &buffer) {
	idx_t prefix_count, suffix_count;
	auto prefix_buffer = ReadDbpData(reader.allocator, buffer, prefix_count);
	auto suffix_buffer = ReadDbpData(reader.allocator, buffer, suffix_count);
	if (prefix_count != suffix_count) {
		throw std::runtime_error("DELTA_BYTE_ARRAY - prefix and suffix counts are different - corrupt file?");
	}
	if (prefix_count == 0) {
		// no values
		byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
		return;
	}
	auto prefix_data = reinterpret_cast<uint32_t *>(prefix_buffer->ptr);
	auto suffix_data = reinterpret_cast<uint32_t *>(suffix_buffer->ptr);
	byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, prefix_count);
	byte_array_count = prefix_count;
	delta_offset = 0;
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t i = 0; i < prefix_count; i++) {
		auto str_len = prefix_data[i] + suffix_data[i];
		buffer.available(suffix_data[i]);
		string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
		auto result_data = string_data[i].GetDataWriteable();
		if (prefix_data[i] > 0) {
			if (i == 0 || prefix_data[i] > string_data[i - 1].GetSize()) {
				throw std::runtime_error("DELTA_BYTE_ARRAY - prefix is out of range - corrupt file?");
			}
			memcpy(result_data, string_data[i - 1].GetData(), prefix_data[i]);
		}
		memcpy(result_data + prefix_data[i], buffer.ptr, suffix_data[i]);
		buffer.inc(suffix_data[i]);
		string_data[i].Finalize();
	}
}

void StringColumnReader::DeltaByteArray(uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
                                        idx_t result_offset, Vector &result) {
	if (!byte_array_data) {
		throw std::runtime_error("Internal error - DeltaByteArray called but there was no byte_array_data set");
	}
	auto result_ptr = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
		if (HasDefines() && defines[row_idx + result_offset] != max_define) {
			result_mask.SetInvalid(row_idx + result_offset);
			continue;
		}
		if (filter.test(row_idx + result_offset)) {
			if (delta_offset >= byte_array_count) {
				throw IOException("DELTA_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
				                  "read of %d from %d entries) - corrupt file?",
				                  delta_offset + 1, byte_array_count);
			}
			result_ptr[row_idx + result_offset] = string_data[delta_offset++];
		} else {
			delta_offset++;
		}
	}
	StringVector::AddHeapReference(result, *byte_array_data);
}

class ParquetStringVectorBuffer : public VectorBuffer {
public:
	explicit ParquetStringVectorBuffer(shared_ptr<ByteBuffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(std::move(buffer_p)) {
	}

private:
	shared_ptr<ByteBuffer> buffer;
};

void StringColumnReader::PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(std::move(plain_data)));
}

string_t StringParquetValueConversion::PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = reader.Cast<StringColumnReader>();
	uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
	plain_data.available(str_len);
	auto plain_str = char_ptr_cast(plain_data.ptr);
	auto actual_str_len = reader.Cast<StringColumnReader>().VerifyString(plain_str, str_len);
	auto ret_str = string_t(plain_str, actual_str_len);
	plain_data.inc(str_len);
	return ret_str;
}

void StringParquetValueConversion::PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = reader.Cast<StringColumnReader>();
	uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
	plain_data.inc(str_len);
}

bool StringParquetValueConversion::PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
	return true;
}

string_t StringParquetValueConversion::UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
	return PlainRead(plain_data, reader);
}

void StringParquetValueConversion::UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
	PlainSkip(plain_data, reader);
}


}
