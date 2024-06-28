//===----------------------------------------------------------------------===//
//                         DuckDB
//
// string_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"

namespace duckdb {

struct StringParquetValueConversion {
	static string_t DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader);

	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader);
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader);

	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count);
	static string_t UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader);
	static void UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader);
};

class StringColumnReader : public TemplatedColumnReader<string_t, StringParquetValueConversion> {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;

public:
	StringColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                   idx_t max_define_p, idx_t max_repeat_p);

	unsafe_unique_ptr<string_t[]> dict_strings;
	idx_t fixed_width_string_length;
	idx_t delta_offset = 0;

public:
	void Dictionary(shared_ptr<ResizeableBuffer> dictionary_data, idx_t num_entries) override;

	void PrepareDeltaLengthByteArray(ResizeableBuffer &buffer) override;
	void PrepareDeltaByteArray(ResizeableBuffer &buffer) override;
	void DeltaByteArray(uint8_t *defines, idx_t num_values, parquet_filter_t &filter, idx_t result_offset,
	                    Vector &result) override;
	static uint32_t VerifyString(const char *str_data, uint32_t str_len, const bool isVarchar);
	uint32_t VerifyString(const char *str_data, uint32_t str_len);

protected:
	void DictReference(Vector &result) override;
	void PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) override;
};

} // namespace duckdb
