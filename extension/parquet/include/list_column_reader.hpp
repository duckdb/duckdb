//===----------------------------------------------------------------------===//
//                         DuckDB
//
// list_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

class ListColumnReader : public ColumnReader {
public:
	ListColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p,
	                 idx_t max_define_p, idx_t max_repeat_p, unique_ptr<ColumnReader> child_column_reader_p);

	idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	           Vector &result_out) override;

	void ApplyPendingSkips(idx_t num_values) override;

	void InitializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override {
		child_column_reader->InitializeRead(columns, protocol_p);
	}

	idx_t GroupRowsAvailable() override {
		return child_column_reader->GroupRowsAvailable() + overflow_child_count;
	}

	uint64_t TotalCompressedSize() override {
		return child_column_reader->TotalCompressedSize();
	}

	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override {
		child_column_reader->RegisterPrefetch(transport, allow_merge);
	}

private:
	unique_ptr<ColumnReader> child_column_reader;
	ResizeableBuffer child_defines;
	ResizeableBuffer child_repeats;
	uint8_t *child_defines_ptr;
	uint8_t *child_repeats_ptr;

	VectorCache read_cache;
	Vector read_vector;

	parquet_filter_t child_filter;

	idx_t overflow_child_count;
};

} // namespace duckdb
