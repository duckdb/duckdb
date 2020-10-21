//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {
class UncompressedSegment;
class BaseStatistics;
class SegmentStatistics;

//! The table data writer is responsible for writing the data of a table to the block manager
class TableDataWriter {
public:
	TableDataWriter(CheckpointManager &manager, TableCatalogEntry &table);
	~TableDataWriter();

	void WriteTableData(ClientContext &context);

private:
	void AppendData(Transaction &transaction, idx_t col_idx, Vector &data, idx_t count);

	void CreateSegment(idx_t col_idx);
	void FlushSegment(Transaction &transaction, idx_t col_idx);

	void WriteDataPointers();
	void VerifyDataPointers();

private:
	CheckpointManager &manager;
	TableCatalogEntry &table;

	vector<unique_ptr<UncompressedSegment>> segments;
	vector<unique_ptr<SegmentStatistics>> stats;
	vector<unique_ptr<BaseStatistics>> column_stats;

	vector<vector<DataPointer>> data_pointers;
};

} // namespace duckdb
