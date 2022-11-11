//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/base_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/base_csv_reader.hpp"

namespace duckdb {
struct CopyInfo;
struct CSVFileHandle;
struct FileHandle;
struct StrpTimeFormat;

class FileOpener;
class FileSystem;

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
	explicit TextSearchShiftArray(string search_term);

	inline bool Match(uint8_t &position, uint8_t byte_value) {
		if (position >= length) {
			return false;
		}
		position = shifts[position * 255 + byte_value];
		return position == length;
	}

	idx_t length;
	unique_ptr<uint8_t[]> shifts;
};

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader : public BaseCSVReader {
	//! Initial buffer read size; can be extended for long lines
	static constexpr idx_t INITIAL_BUFFER_SIZE = 16384;
	//! Larger buffer size for non disk files
	static constexpr idx_t INITIAL_BUFFER_SIZE_LARGE = 10000000; // 10MB

public:
	BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	BufferedCSVReader(FileSystem &fs, Allocator &allocator, FileOpener *opener, BufferedCSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	~BufferedCSVReader();

	unique_ptr<char[]> buffer;
	idx_t buffer_size;
	idx_t position;
	idx_t start = 0;

	vector<unique_ptr<char[]>> cached_buffers;

	unique_ptr<CSVFileHandle> file_handle;

	TextSearchShiftArray delimiter_search, escape_search, quote_search;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Skips skip_rows, reads header row from input stream
	void SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header);
	//! Jumps back to the beginning of input stream and resets necessary internal states
	void JumpToBeginning(idx_t skip_rows, bool skip_header);
	//! Resets the buffer
	void ResetBuffer();
	//! Resets the steam
	void ResetStream();
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(idx_t &start);
	//! Jumps back to the beginning of input stream and resets necessary internal states
	bool JumpToNextSample();
	//! Initializes the TextSearchShiftArrays for complex parser
	void PrepareComplexParser();
	//! Try to parse a single datachunk from the file. Throws an exception if anything goes wrong.
	void ParseCSV(ParserMode mode);
	//! Try to parse a single datachunk from the file. Returns whether or not the parsing is successful
	bool TryParseCSV(ParserMode mode);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	bool TryParseCSV(ParserMode mode, DataChunk &insert_chunk, string &error_message);

	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message);
	//! Parses more complex CSV files with multi-byte delimiters, escapes or quotes
	bool TryParseComplexCSV(DataChunk &insert_chunk, string &error_message);
	//! Sniffs CSV dialect and determines skip rows, header row, column types and column names
	vector<LogicalType> SniffCSV(const vector<LogicalType> &requested_types);

	//! First phase of auto detection: detect CSV dialect (i.e. delimiter, quote rules, etc)
	void DetectDialect(const vector<LogicalType> &requested_types, BufferedCSVReaderOptions &original_options,
	                   vector<BufferedCSVReaderOptions> &info_candidates, idx_t &best_num_cols);
	//! Second phase of auto detection: detect candidate types for each column
	void DetectCandidateTypes(const vector<LogicalType> &type_candidates,
	                          const map<LogicalTypeId, vector<const char *>> &format_template_candidates,
	                          const vector<BufferedCSVReaderOptions> &info_candidates,
	                          BufferedCSVReaderOptions &original_options, idx_t best_num_cols,
	                          vector<vector<LogicalType>> &best_sql_types_candidates,
	                          std::map<LogicalTypeId, vector<string>> &best_format_candidates,
	                          DataChunk &best_header_row);
	//! Third phase of auto detection: detect header of CSV file
	void DetectHeader(const vector<vector<LogicalType>> &best_sql_types_candidates, const DataChunk &best_header_row);
	//! Fourth phase of auto detection: refine the types of each column and select which types to use for each column
	vector<LogicalType> RefineTypeDetection(const vector<LogicalType> &type_candidates,
	                                        const vector<LogicalType> &requested_types,
	                                        vector<vector<LogicalType>> &best_sql_types_candidates,
	                                        map<LogicalTypeId, vector<string>> &best_format_candidates);
};

} // namespace duckdb
