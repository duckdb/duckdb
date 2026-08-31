#include "reader/byte_array_length_column_reader.hpp"

#include "parquet_column_schema.hpp"

namespace duckdb {

ByteArrayLengthColumnReader::ByteArrayLengthColumnReader(const ParquetReader &reader, const ParquetColumnSchema &schema)
    : ColumnReader(reader, schema), fixed_width_string_length(DConstants::INVALID_INDEX) {
	if (schema.parquet_type == Type::FIXED_LEN_BYTE_ARRAY) {
		fixed_width_string_length = schema.type_length;
	}
}

void ByteArrayLengthColumnReader::Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                                        idx_t result_offset, Vector &result) {
	PlainTemplated<int64_t, ByteArrayLengthValueConversion>(*plain_data, defines, num_values, result_offset, result);
}

void ByteArrayLengthColumnReader::PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) {
	PlainSkipTemplated<ByteArrayLengthValueConversion>(plain_data, defines, num_values);
}

void ByteArrayLengthColumnReader::PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines,
                                              idx_t num_values, Vector &result, const SelectionVector &sel,
                                              idx_t count) {
	PlainSelectTemplated<int64_t, ByteArrayLengthValueConversion>(*plain_data, defines, num_values, result, sel, count);
}

} // namespace duckdb
