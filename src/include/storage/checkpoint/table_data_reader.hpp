//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/checkpoint/table_data_reader.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "storage/checkpoint_manager.hpp"

namespace duckdb {

//! The table data reader is responsible for reading the data of a table from the block manager
class TableDataReader {
public:
	TableDataReader(CheckpointManager &manager);

	void ReadTableData(ClientContext &context, TableCatalogEntry &table, MetaBlockReader &reader);

	bool ReadBlock(uint64_t col);
	void ReadString(Vector &vector, uint64_t col);
	void ReadDataPointers(uint64_t column_count, MetaBlockReader &reader);
private:
	CheckpointManager &manager;

	vector<unique_ptr<Block>> blocks;
	vector<uint64_t> offsets;
	vector<uint64_t> tuple_counts;
	vector<uint64_t> row_numbers;
	vector<uint64_t> indexes;

	vector<vector<DataPointer>> data_pointers;
};

} // namespace duckdb
