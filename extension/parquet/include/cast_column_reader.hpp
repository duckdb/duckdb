//===----------------------------------------------------------------------===//
//                         DuckDB
//
// cast_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "templated_column_reader.hpp"

namespace duckdb {

//! A column reader that represents a cast over a child reader
class CastColumnReader : public ColumnReader {
public:
	CastColumnReader(unique_ptr<ColumnReader> child_reader, LogicalType target_type);

	unique_ptr<ColumnReader> child_reader;
	DataChunk intermediate_chunk;

public:
	unique_ptr<BaseStatistics> Stats(const std::vector<ColumnChunk> &columns) override;
	void InitializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	           Vector &result) override;

	void Skip(idx_t num_values) override;
	idx_t GroupRowsAvailable() override;

	uint64_t TotalCompressedSize() override {
		return child_reader->TotalCompressedSize();
	}

	idx_t FileOffset() const override {
		return child_reader->FileOffset();
	}

	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override {
		child_reader->RegisterPrefetch(transport, allow_merge);
	}
};

} // namespace duckdb
