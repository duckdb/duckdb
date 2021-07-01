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
	                   idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<string_t, StringParquetValueConversion>(reader, move(type_p), schema_p, schema_idx_p,
	                                                                    max_define_p, max_repeat_p) {
		fixed_width_string_length = 0;
		if (schema_p.type == Type::FIXED_LEN_BYTE_ARRAY) {
			D_ASSERT(schema_p.__isset.type_length);
			fixed_width_string_length = schema_p.type_length;
		}
	};

	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) override;

	unique_ptr<string_t[]> dict_strings;
	void VerifyString(const char *str_data, idx_t str_len);
	idx_t fixed_width_string_length;

protected:
	void DictReference(Vector &result) override;
	void PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) override;
};

} // namespace duckdb