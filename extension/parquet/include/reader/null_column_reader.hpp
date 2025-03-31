//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/null_column_reader.hpp
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
	NullColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema) : ColumnReader(reader, schema) {};

	shared_ptr<ResizeableBuffer> dict;

public:
	void Plain(ByteBuffer &plain_data, uint8_t *defines, uint64_t num_values, idx_t result_offset,
	           Vector &result) override {
		(void)defines;
		(void)plain_data;

		auto &result_mask = FlatVector::Validity(result);
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			result_mask.SetInvalid(row_idx + result_offset);
		}
	}
};

} // namespace duckdb
