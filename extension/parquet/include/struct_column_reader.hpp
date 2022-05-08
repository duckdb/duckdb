//===----------------------------------------------------------------------===//
//                         DuckDB
//
// struct_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

class StructColumnReader : public ColumnReader {
public:
	StructColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                   idx_t max_define_p, idx_t max_repeat_p, vector<unique_ptr<ColumnReader>> child_readers_p);

	vector<unique_ptr<ColumnReader>> child_readers;

public:
	ColumnReader *GetChildReader(idx_t child_idx);

	void InitializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	           Vector &result) override;

	void Skip(idx_t num_values) override;
	idx_t GroupRowsAvailable() override;
	uint64_t TotalCompressedSize() override;
	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override;
};

} // namespace duckdb
