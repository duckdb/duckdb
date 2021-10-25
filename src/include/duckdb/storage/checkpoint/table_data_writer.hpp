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
	                MetaBlockWriter &meta_writer);
	~TableDataWriter();

	BlockPointer WriteTableData();

	MetaBlockWriter &GetMetaWriter() {
		return meta_writer;
	}

	CheckpointManager &GetCheckpointManager() {
		return checkpoint_manager;
	}

	CompressionType GetColumnCompressionType(idx_t i);

private:
	CheckpointManager &checkpoint_manager;
	TableCatalogEntry &table;
	MetaBlockWriter &meta_writer;
};

} // namespace duckdb
