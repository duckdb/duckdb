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
	static constexpr const PhysicalType TYPE = PhysicalType::INVALID;

public:
	CastColumnReader(unique_ptr<ColumnReader> child_reader, LogicalType target_type);

	unique_ptr<ColumnReader> child_reader;
	DataChunk intermediate_chunk;

public:
	unique_ptr<BaseStatistics> Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) override;
	void InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	idx_t Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out, data_ptr_t repeat_out,
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
