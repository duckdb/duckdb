//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/buffered_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {
struct CopyInfo;

//! The shifts array allows for linear searching of multi-byte values. For each position, it determines the next
//! position given that we encounter a byte with the given value.
/*! For example, if we have a string "ABAC", the shifts array will have the following values:
 *  [0] --> ['A'] = 1, all others = 0
 *  [1] --> ['B'] = 2, ['A'] = 1, all others = 0
 *  [2] --> ['A'] = 3, all others = 0
 *  [3] --> ['C'] = 4 (match), 'B' = 2, 'A' = 1, all others = 0
 * Suppose we then search in the following string "ABABAC", our progression will be as follows:
 * 'A' -> [1], 'B' -> [2], 'A' -> [3], 'B' -> [2], 'A' -> [3], 'C' -> [4] (match!)
 */
struct TextSearchShiftArray {
	TextSearchShiftArray(string search_term);

	inline bool Match(uint8_t &position, uint8_t byte_value) {
		position = shifts[position * 255 + byte_value];
		return position == length;
	}

	index_t length;
	unique_ptr<uint8_t[]> shifts;
};

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

	TextSearchShiftArray delimiter_search, escape_search, quote_search;

	vector<unique_ptr<char[]>> cached_buffers;

	DataChunk parse_chunk;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

private:
	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	void ParseSimpleCSV(DataChunk &insert_chunk);
	//! Parses more complex CSV files with multi-byte delimiters, escapes or quotes
	void ParseComplexCSV(DataChunk &insert_chunk);

	//! Adds a value to the current row
	void AddValue(char *str_val, index_t length, index_t &column, vector<index_t> &escape_positions);
	//! Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row being added
	bool AddRow(DataChunk &insert_chunk, index_t &column);
	//! Finalizes a chunk, parsing all values that have been added so far and adding them to the insert_chunk
	void Flush(DataChunk &insert_chunk);
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(index_t &start);
};

} // namespace duckdb
