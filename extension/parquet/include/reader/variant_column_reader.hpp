//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/variant_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

class VariantColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::STRUCT;

public:
	VariantColumnReader(ClientContext &context, ParquetReader &reader, const ParquetColumnSchema &schema,
	                    vector<unique_ptr<ColumnReader>> child_readers_p);

	ClientContext &context;
	vector<unique_ptr<ColumnReader>> child_readers;

public:
	ColumnReader &GetChildReader(idx_t child_idx);

	void InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	idx_t Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) override;

	void Skip(idx_t num_values) override;
	idx_t GroupRowsAvailable() override;
	uint64_t TotalCompressedSize() override;
	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override;

protected:
	idx_t metadata_reader_idx;
	idx_t value_reader_idx;
};

} // namespace duckdb
