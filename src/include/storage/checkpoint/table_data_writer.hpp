//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/checkpoint/table_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/checkpoint_manager.hpp"
#include "common/unordered_map.hpp"

namespace duckdb {

struct StringDictionary {
	unordered_map<string, index_t> offsets;
	index_t size = 0;
};

//! The table data writer is responsible for writing the data of a table to the block manager
class TableDataWriter {
public:
	static constexpr index_t BLOCK_HEADER_NUMERIC = 0;
	static constexpr index_t BLOCK_HEADER_STRING = sizeof(int32_t);
	//! Marker indicating that a string is a big string (bigger than block size), and located in a special big string
	//! area
	static constexpr char BIG_STRING_MARKER[2] = {'\201', '\0'};
	static constexpr index_t BIG_STRING_MARKER_SIZE = 2 * sizeof(char) + sizeof(block_id_t);

public:
	TableDataWriter(CheckpointManager &manager, TableCatalogEntry &table);

	void WriteTableData(Transaction &transaction);

	void WriteColumnData(DataChunk &chunk, index_t column_index);
	void WriteString(index_t index, const char *val);
	void FlushBlock(index_t col);

	void WriteDataPointers();

private:
	//! Flush the block of the column if it cannot fit write_size more bytes
	void FlushIfFull(index_t col, index_t write_size);
	//! Writes the dictionary to the block buffer
	void FlushDictionary(index_t col);

	CheckpointManager &manager;
	TableCatalogEntry &table;

	vector<unique_ptr<Block>> blocks;
	vector<index_t> offsets;
	vector<index_t> tuple_counts;
	vector<index_t> row_numbers;
	vector<index_t> indexes;
	vector<StringDictionary> dictionaries;

	vector<vector<DataPointer>> data_pointers;
};

} // namespace duckdb
