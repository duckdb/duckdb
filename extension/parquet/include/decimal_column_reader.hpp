//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decimal_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

template <class DUCKDB_PHYSICAL_TYPE>
struct DecimalParquetValueConversion {
	static DUCKDB_PHYSICAL_TYPE DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		auto dict_ptr = (DUCKDB_PHYSICAL_TYPE *)dict.ptr;
		return dict_ptr[offset];
	}

	static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		DUCKDB_PHYSICAL_TYPE res = 0;
		auto byte_len = (idx_t)reader.Schema().type_length; /* sure, type length needs to be a signed int */
		D_ASSERT(byte_len <= sizeof(DUCKDB_PHYSICAL_TYPE));
		plain_data.available(byte_len);
		auto res_ptr = (uint8_t *)&res;

		// numbers are stored as two's complement so some muckery is required
		bool positive = (*plain_data.ptr & 0x80) == 0;

		for (idx_t i = 0; i < byte_len; i++) {
			auto byte = *(plain_data.ptr + (byte_len - i - 1));
			res_ptr[i] = positive ? byte : byte ^ 0xFF;
		}
		plain_data.inc(byte_len);
		if (!positive) {
			res += 1;
			return -res;
		}
		return res;
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(reader.Schema().type_length);
	}
};

template <class DUCKDB_PHYSICAL_TYPE>
class DecimalColumnReader
    : public TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE, DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE>> {

public:
	DecimalColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	                    idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE, DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE>>(
	          reader, move(type_p), schema_p, file_idx_p, max_define_p, max_repeat_p) {};

protected:
	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) {
		this->dict = make_shared<ResizeableBuffer>(this->reader.allocator, num_entries * sizeof(DUCKDB_PHYSICAL_TYPE));
		auto dict_ptr = (DUCKDB_PHYSICAL_TYPE *)this->dict->ptr;
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] = DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE>::PlainRead(*dictionary_data, *this);
		}
	}
};

} // namespace duckdb