//===----------------------------------------------------------------------===//
//                         DuckDB
//
// templated__column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"

namespace duckdb {

template <class VALUE_TYPE>
struct TemplatedParquetValueConversion {
	static VALUE_TYPE DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		D_ASSERT(offset < dict.len / sizeof(VALUE_TYPE));
		return ((VALUE_TYPE *)dict.ptr)[offset];
	}

	static VALUE_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		return plain_data.read<VALUE_TYPE>();
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(sizeof(VALUE_TYPE));
	}
};

template <class VALUE_TYPE, class VALUE_CONVERSION>
class TemplatedColumnReader : public ColumnReader {
public:
	TemplatedColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                      idx_t max_define_p, idx_t max_repeat_p)
	    : ColumnReader(reader, move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {};

	shared_ptr<ByteBuffer> dict;

public:
	void Dictionary(shared_ptr<ByteBuffer> data, idx_t num_entries) override {
		dict = move(data);
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	             idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		auto &result_mask = FlatVector::Validity(result);

		idx_t offset_idx = 0;
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (HasDefines() && defines[row_idx + result_offset] != max_define) {
				result_mask.SetInvalid(row_idx + result_offset);
				continue;
			}
			if (filter[row_idx + result_offset]) {
				VALUE_TYPE val = VALUE_CONVERSION::DictRead(*dict, offsets[offset_idx++], *this);
				result_ptr[row_idx + result_offset] = val;
			} else {
				offset_idx++;
			}
		}
	}

	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	           idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (HasDefines() && defines[row_idx + result_offset] != max_define) {
				result_mask.SetInvalid(row_idx + result_offset);
				continue;
			}
			if (filter[row_idx + result_offset]) {
				VALUE_TYPE val = VALUE_CONVERSION::PlainRead(*plain_data, *this);
				result_ptr[row_idx + result_offset] = val;
			} else { // there is still some data there that we have to skip over
				VALUE_CONVERSION::PlainSkip(*plain_data, *this);
			}
		}
	}
};

template <class PARQUET_PHYSICAL_TYPE, class DUCKDB_PHYSICAL_TYPE,
          DUCKDB_PHYSICAL_TYPE (*FUNC)(const PARQUET_PHYSICAL_TYPE &input)>
struct CallbackParquetValueConversion {
	static DUCKDB_PHYSICAL_TYPE DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		return TemplatedParquetValueConversion<DUCKDB_PHYSICAL_TYPE>::DictRead(dict, offset, reader);
	}

	static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		return FUNC(plain_data.read<PARQUET_PHYSICAL_TYPE>());
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(sizeof(PARQUET_PHYSICAL_TYPE));
	}
};

} // namespace duckdb
