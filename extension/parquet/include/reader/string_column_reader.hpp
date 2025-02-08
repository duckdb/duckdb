//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/string_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

struct StringParquetValueConversion {
	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader);
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader);
	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count);
	static string_t UnsafePlainRead(ByteBuffer &plain_data, ColumnReader &reader);
	static void UnsafePlainSkip(ByteBuffer &plain_data, ColumnReader &reader);
};

class StringColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;

public:
	StringColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                   idx_t max_define_p, idx_t max_repeat_p);
	idx_t fixed_width_string_length;

public:
	static void VerifyString(const char *str_data, uint32_t str_len, const bool isVarchar);
	void VerifyString(const char *str_data, uint32_t str_len);

protected:
	void Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override {
		throw NotImplementedException("StringColumnReader can only read plain data from a shared buffer");
	}
	void Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override;
	void PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) override;
};

} // namespace duckdb
