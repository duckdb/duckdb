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
class CheckpointManager;
class ColumnData;
class ColumnSegment;
class RowGroup;
class BaseStatistics;
class SegmentStatistics;

//! The table data writer is responsible for writing the data of a table to the block manager
class TableDataWriter {
	friend class ColumnData;

public:
	TableDataWriter(DatabaseInstance &db, CheckpointManager &checkpoint_manager, TableCatalogEntry &table,
	                MetaBlockWriter &table_data_writer, MetaBlockWriter &meta_data_writer);
	~TableDataWriter();

	void WriteTableData();

	MetaBlockWriter &GetTableWriter() {
		return table_data_writer;
	}
	MetaBlockWriter &GetMetaWriter() {
		return meta_data_writer;
	}

	CheckpointManager &GetCheckpointManager() {
		return checkpoint_manager;
	}

	CompressionType GetColumnCompressionType(idx_t i);

private:
	CheckpointManager &checkpoint_manager;
	TableCatalogEntry &table;
	// Writes the actual table data
	MetaBlockWriter &table_data_writer;
	// Writes the metadata of the table
	MetaBlockWriter &meta_data_writer;
};

} // namespace duckdb
