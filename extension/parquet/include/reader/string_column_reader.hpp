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
	enum class StringColumnType : uint8_t { VARCHAR, JSON, OTHER };

	static StringColumnType GetStringColumnType(const LogicalType &type) {
		if (type.IsJSONType()) {
			return StringColumnType::JSON;
		}
		if (type.id() == LogicalTypeId::VARCHAR) {
			return StringColumnType::VARCHAR;
		}
		return StringColumnType::OTHER;
	}

public:
	static constexpr const PhysicalType TYPE = PhysicalType::VARCHAR;

public:
	StringColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema);
	idx_t fixed_width_string_length;
	const StringColumnType string_column_type;

public:
	static void VerifyString(const char *str_data, uint32_t str_len, const bool isVarchar);
	void VerifyString(const char *str_data, uint32_t str_len) const;

	static void ReferenceBlock(Vector &result, shared_ptr<ResizeableBuffer> &block);

protected:
	void Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override {
		throw NotImplementedException("StringColumnReader can only read plain data from a shared buffer");
	}
	void Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset,
	           Vector &result) override;
	void PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) override;
	void PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values, Vector &result,
	                 const SelectionVector &sel, idx_t count) override;

	bool SupportsDirectFilter() const override {
		return true;
	}
	bool SupportsDirectSelect() const override {
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
