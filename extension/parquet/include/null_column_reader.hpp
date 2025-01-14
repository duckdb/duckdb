//===----------------------------------------------------------------------===//
//                         DuckDB
//
// null_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

class NullColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::INVALID;

public:
	NullColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                 idx_t max_define_p, idx_t max_repeat_p)
	    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {};

	shared_ptr<ResizeableBuffer> dict;

public:
	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, uint64_t num_values, parquet_filter_t *filter,
	           idx_t result_offset, Vector &result) override {
		(void)defines;
		(void)plain_data;
		(void)filter;

		auto &result_mask = FlatVector::Validity(result);
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			result_mask.SetInvalid(row_idx + result_offset);
		}
	}
};

} // namespace duckdb
