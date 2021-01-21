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
class ColumnData;
class UncompressedSegment;
class BaseStatistics;
class SegmentStatistics;

//! The table data writer is responsible for writing the data of a table to the block manager
class TableDataWriter {
	friend class ColumnData;
public:
	TableDataWriter(DatabaseInstance &db, TableCatalogEntry &table, MetaBlockWriter &meta_writer);
	~TableDataWriter();

	void WriteTableData();

	void CheckpointColumn(ColumnData &col_data, idx_t col_idx);
private:
	void AppendData(SegmentTree &new_tree, idx_t col_idx, Vector &data, idx_t count);

	void CreateSegment(idx_t col_idx);
	void FlushSegment(SegmentTree &new_tree, idx_t col_idx);

	void WriteDataPointers();
	void VerifyDataPointers();

private:
	DatabaseInstance &db;
	TableCatalogEntry &table;
	MetaBlockWriter &meta_writer;

	vector<unique_ptr<UncompressedSegment>> segments;
	vector<unique_ptr<SegmentStatistics>> stats;
	vector<unique_ptr<BaseStatistics>> column_stats;

	vector<vector<DataPointer>> data_pointers;
};

} // namespace duckdb
