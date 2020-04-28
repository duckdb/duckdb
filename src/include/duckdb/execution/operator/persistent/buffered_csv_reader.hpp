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

#define SAMPLE_CHUNK_SIZE 100
#if STANDARD_VECTOR_SIZE < SAMPLE_CHUNK_SIZE
#undef SAMPLE_CHUNK_SIZE
#define SAMPLE_CHUNK_SIZE STANDARD_VECTOR_SIZE
#endif

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
	TextSearchShiftArray();
	TextSearchShiftArray(string search_term);

	inline bool Match(uint8_t &position, uint8_t byte_value) {
		position = shifts[position * 255 + byte_value];
		return position == length;
	}

	idx_t length;
	unique_ptr<uint8_t[]> shifts;
};

enum class QuoteRule : uint8_t { QUOTES_RFC = 0, QUOTES_OTHER = 1, NO_QUOTES = 2 };

enum class ParserMode : uint8_t { PARSING = 0, SNIFFING_DIALECT = 1, SNIFFING_DATATYPES = 2 };

static DataChunk DUMMY_CHUNK;

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader {
	static constexpr idx_t INITIAL_BUFFER_SIZE = 16384;
	static constexpr idx_t MAXIMUM_CSV_LINE_SIZE = 1048576;
	static constexpr uint8_t MAX_SAMPLE_CHUNKS = 10;
	ParserMode mode;

public:
	BufferedCSVReader(ClientContext &context, CopyInfo &info, vector<SQLType> requested_types = vector<SQLType>());
	BufferedCSVReader(CopyInfo &info, vector<SQLType> requested_types, unique_ptr<std::istream> source);

	CopyInfo &info;
	vector<SQLType> sql_types;
	vector<string> col_names;
	unique_ptr<std::istream> source;
	bool plain_file_source = false;
	idx_t file_size = 0;

	unique_ptr<char[]> buffer;
	idx_t buffer_size;
	idx_t position;
	idx_t start = 0;

	idx_t linenr = 0;
	bool linenr_estimated = false;

	vector<idx_t> sniffed_column_counts;
	uint8_t sample_chunk_idx = 0;
	bool jumping_samples = false;

	idx_t bytes_in_chunk = 0;
	double bytes_per_line_avg = 0;

	vector<unique_ptr<char[]>> cached_buffers;

	TextSearchShiftArray delimiter_search, escape_search, quote_search;

	DataChunk parse_chunk;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

private:
	//! Initialize Parser
	void Initialize(vector<SQLType> requested_types);
	//! Initializes the parse_chunk with varchar columns and aligns info with new number of cols
	void InitParseChunk(idx_t num_cols);
	//! Initializes the TextSearchShiftArrays for complex parser
	void PrepareComplexParser();
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(ParserMode mode, DataChunk &insert_chunk = DUMMY_CHUNK);
	//! Sniffs CSV dialect and determines skip rows, header row, column types and column names
	vector<SQLType> SniffCSV(vector<SQLType> requested_types);
	//! Skips header rows and skip_rows in the input stream
	void SkipHeader();
	//! Jumps back to the beginning of input stream and resets necessary internal states
	void JumpToBeginning();
	//! Jumps back to the beginning of input stream and resets necessary internal states
	bool JumpToNextSample();
	//! Resets the buffer
	void ResetBuffer();
	//! Resets the steam
	void ResetStream();
	//! Resets the parse_chunk and related internal states, keep_types keeps the parse_chunk initialized
	void ResetParseChunk();

	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	void ParseSimpleCSV(DataChunk &insert_chunk);
	//! Parses more complex CSV files with multi-byte delimiters, escapes or quotes
	void ParseComplexCSV(DataChunk &insert_chunk);

	//! Adds a value to the current row
	void AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions);
	//! Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row being added
	bool AddRow(DataChunk &insert_chunk, idx_t &column);
	//! Finalizes a chunk, parsing all values that have been added so far and adding them to the insert_chunk
	void Flush(DataChunk &insert_chunk);
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(idx_t &start);

	unique_ptr<std::istream> OpenCSV(ClientContext &context, CopyInfo &info);
};

} // namespace duckdb
