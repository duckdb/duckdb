//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/persistent/buffered_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"
#include "parser/parsed_data/copy_info.hpp"

namespace duckdb {
struct CopyInfo;

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader {
	static constexpr index_t INITIAL_BUFFER_SIZE = 16384;
	static constexpr index_t MAXIMUM_CSV_LINE_SIZE = 1048576;

public:
	BufferedCSVReader(CopyInfo &info, vector<SQLType> sql_types, std::istream &source);

	CopyInfo &info;
	vector<SQLType> sql_types;
	std::istream &source;

	unique_ptr<char[]> buffer;
	index_t buffer_size;
	index_t position;
	index_t start = 0;

	index_t linenr = 0;
	index_t nr_elements = 0;

	vector<unique_ptr<char[]>> cached_buffers;

	DataChunk parse_chunk;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

private:
	//! Adds a value to the current row
	void AddValue(char *str_val, index_t length, index_t &column);
	//! Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row being added
	bool AddRow(DataChunk &insert_chunk, index_t &column);
	//! Finalizes a chunk, parsing all values that have been added so far and adding them to the insert_chunk
	void Flush(DataChunk &insert_chunk);
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(index_t &start);
};

} // namespace duckdb
