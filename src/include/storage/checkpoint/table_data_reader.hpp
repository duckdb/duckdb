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
	TableDataReader(CheckpointManager &manager, TableCatalogEntry &table, MetaBlockReader &reader);

	void ReadTableData(ClientContext &context);

	bool ReadBlock(index_t col);
	void ReadString(Vector &vector, index_t col);
	void ReadDataPointers(index_t column_count);

private:
	CheckpointManager &manager;
	TableCatalogEntry &table;
	MetaBlockReader &reader;

	vector<unique_ptr<Block>> blocks;
	vector<index_t> offsets;
	vector<index_t> tuple_counts;
	vector<index_t> row_numbers;
	vector<index_t> indexes;
	vector<data_ptr_t> dictionary_start;

	vector<vector<DataPointer>> data_pointers;
};

} // namespace duckdb
