//===----------------------------------------------------------------------===//
//                         DuckDB
//
// generated_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/limits.hpp"
#endif
#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

//! Reads a file-absolute row number as a virtual column that's not actually stored in the file
class RowNumberColumnReader : public ColumnReader {
public:
	RowNumberColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                      idx_t max_define_p, idx_t max_repeat_p);

public:
	idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	           Vector &result) override;

	unique_ptr<BaseStatistics> Stats(idx_t row_group_idx_p, const std::vector<ColumnChunk> &columns) override;

	void InitializeRead(idx_t row_group_idx_p, const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	void Skip(idx_t num_values) override {
		row_group_offset += num_values;
	}
	idx_t GroupRowsAvailable() override {
		return NumericLimits<idx_t>::Maximum();
	};
	uint64_t TotalCompressedSize() override {
		return 0;
	}
	idx_t FileOffset() const override {
		return 0;
	}
	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override {
	}

private:
	idx_t row_group_offset;
};

} // namespace duckdb
