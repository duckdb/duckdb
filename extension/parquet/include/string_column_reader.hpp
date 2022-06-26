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
};

class StringColumnReader : public TemplatedColumnReader<string_t, StringParquetValueConversion> {
public:
	StringColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                   idx_t max_define_p, idx_t max_repeat_p);

	unique_ptr<string_t[]> dict_strings;
	idx_t fixed_width_string_length;

public:
	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) override;

	uint32_t VerifyString(const char *str_data, uint32_t str_len);

protected:
	void DictReference(Vector &result) override;
	void PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) override;
};

} // namespace duckdb
