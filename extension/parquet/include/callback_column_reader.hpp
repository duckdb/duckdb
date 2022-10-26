//===----------------------------------------------------------------------===//
//                         DuckDB
//
// callback_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

template <class PARQUET_PHYSICAL_TYPE, class DUCKDB_PHYSICAL_TYPE,
          DUCKDB_PHYSICAL_TYPE (*FUNC)(const PARQUET_PHYSICAL_TYPE &input)>
class CallbackColumnReader
    : public TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
                                   CallbackParquetValueConversion<PARQUET_PHYSICAL_TYPE, DUCKDB_PHYSICAL_TYPE, FUNC>> {

public:
	CallbackColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	                     idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
	                            CallbackParquetValueConversion<PARQUET_PHYSICAL_TYPE, DUCKDB_PHYSICAL_TYPE, FUNC>>(
	          reader, move(type_p), schema_p, file_idx_p, max_define_p, max_repeat_p) {
	}

protected:
	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) {
		this->dict = make_shared<ResizeableBuffer>(this->reader.allocator, num_entries * sizeof(DUCKDB_PHYSICAL_TYPE));
		auto dict_ptr = (DUCKDB_PHYSICAL_TYPE *)this->dict->ptr;
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] = FUNC(dictionary_data->read<PARQUET_PHYSICAL_TYPE>());
		}
	}
};

} // namespace duckdb
