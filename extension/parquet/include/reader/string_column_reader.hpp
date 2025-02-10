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

	bool SupportsDirectFilter() const override {
		return true;
	}
};

struct StringParquetValueConversion {
	template <bool CHECKED>
	static string_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		auto &scr = reader.Cast<StringColumnReader>();
		uint32_t str_len =
		    scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
		plain_data.available(str_len);
		auto plain_str = char_ptr_cast(plain_data.ptr);
		scr.VerifyString(plain_str, str_len);
		auto ret_str = string_t(plain_str, str_len);
		plain_data.inc(str_len);
		return ret_str;
	}
	template <bool CHECKED>
	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		auto &scr = reader.Cast<StringColumnReader>();
		uint32_t str_len =
		    scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
		plain_data.inc(str_len);
	}
	static bool PlainAvailable(const ByteBuffer &plain_data, const idx_t count) {
		return false;
	}

	static idx_t PlainConstantSize() {
		return 0;
	}
};

} // namespace duckdb
