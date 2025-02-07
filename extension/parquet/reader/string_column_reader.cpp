#include "reader/string_column_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// String Column Reader
//===--------------------------------------------------------------------===//
StringColumnReader::StringColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                       idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {
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

class ParquetStringVectorBuffer : public VectorBuffer {
public:
	explicit ParquetStringVectorBuffer(shared_ptr<ResizeableBuffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(std::move(buffer_p)) {
	}

private:
	shared_ptr<ResizeableBuffer> buffer;
};

void StringColumnReader::Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                               parquet_filter_t *filter, idx_t result_offset, Vector &result) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(plain_data));
	PlainTemplated<string_t, StringParquetValueConversion>(*plain_data, defines, num_values, filter, result_offset,
	                                                       result);
}

void StringColumnReader::PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) {
	PlainSkipTemplated<StringParquetValueConversion>(plain_data, defines, num_values);
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

} // namespace duckdb
