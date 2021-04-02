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
class MorselInfo;
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
	void CheckpointDeletes(MorselInfo *info);

	MetaBlockWriter &GetMetaWriter() {
		return meta_writer;
	}

private:
	DatabaseInstance &db;
	TableCatalogEntry &table;
	MetaBlockWriter &meta_writer;
};

} // namespace duckdb
