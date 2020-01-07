//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/table_data_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint_manager.hpp"

namespace duckdb {
struct BoundCreateTableInfo;

//! The table data reader is responsible for reading the data of a table from the block manager
class TableDataReader {
public:
	TableDataReader(CheckpointManager &manager, MetaBlockReader &reader, BoundCreateTableInfo &info);

	void ReadTableData();

private:
	CheckpointManager &manager;
	MetaBlockReader &reader;
	BoundCreateTableInfo &info;
};

} // namespace duckdb
