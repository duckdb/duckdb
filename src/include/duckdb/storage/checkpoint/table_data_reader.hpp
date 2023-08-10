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
	TableDataReader(MetadataReader &reader, BoundCreateTableInfo &info);

	void ReadTableData();

private:
	MetadataReader &reader;
	BoundCreateTableInfo &info;
};

} // namespace duckdb
