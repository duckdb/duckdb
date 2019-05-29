//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/checkpoint/table_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "storage/checkpoint_manager.hpp"

namespace duckdb {

//! The table data writer is responsible for writing the data of a table to the block manager
class TableDataWriter {
public:
	TableDataWriter(CheckpointManager &manager);

	void WriteTableData(Transaction &transaction, TableCatalogEntry &table);

	void WriteColumnData(DataChunk &chunk, uint64_t column_index);
	void WriteString(uint64_t index, const char *val);
	void FlushBlock(uint64_t col);

	void WriteDataPointers();
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
