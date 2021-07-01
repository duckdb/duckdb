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
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"

#include <map>
#include <sstream>
#include <queue>

namespace duckdb {
struct CopyInfo;
struct FileHandle;
struct StrpTimeFormat;

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

struct BufferedCSVReaderOptions {
	//! The file path of the CSV file to read
	string file_path;
	//! Whether file is compressed or not, and if so which compression type
	//! ("infer" (default; infer from file extention), "gzip", "none")
	string compression = "infer";
	//! Whether or not to automatically detect dialect and datatypes
	bool auto_detect = false;
	//! Whether or not a delimiter was defined by the user
	bool has_delimiter = false;
	//! Delimiter to separate columns within each line
	string delimiter = ",";
	//! Whether or not a quote sign was defined by the user
	bool has_quote = false;
	//! Quote used for columns that contain reserved characters, e.g., delimiter
	string quote = "\"";
	//! Whether or not an escape character was defined by the user
	bool has_escape = false;
	//! Escape character to escape quote character
	string escape;
	//! Whether or not a header information was given by the user
	bool has_header = false;
	//! Whether or not the file has a header line
	bool header = false;
	//! Whether or not header names shall be normalized
	bool normalize_names = false;
	//! How many leading rows to skip
	idx_t skip_rows = 0;
	//! Expected number of columns
	idx_t num_cols = 0;
	//! Specifies the string that represents a null value
	string null_str;
	//! True, if column with that index must skip null check
	vector<bool> force_not_null;
	//! Size of sample chunk used for dialect and type detection
	idx_t sample_chunk_size = STANDARD_VECTOR_SIZE;
	//! Number of sample chunks used for type detection
	idx_t sample_chunks = 10;
	//! Number of samples to buffer
	idx_t buffer_size = STANDARD_VECTOR_SIZE * 100;
	//! Consider all columns to be of type varchar
	bool all_varchar = false;
	//! The date format to use (if any is specified)
	std::map<LogicalTypeId, StrpTimeFormat> date_format = {{LogicalTypeId::DATE, {}}, {LogicalTypeId::TIMESTAMP, {}}};
	//! Whether or not a type format is specified
	std::map<LogicalTypeId, bool> has_format = {{LogicalTypeId::DATE, false}, {LogicalTypeId::TIMESTAMP, false}};

	std::string toString() const {
		return "DELIMITER='" + delimiter + (has_delimiter ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
		       ", QUOTE='" + quote + (has_quote ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
		       ", ESCAPE='" + escape + (has_escape ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
		       ", HEADER=" + std::to_string(header) +
		       (has_header ? "" : (auto_detect ? " (auto detected)" : "' (default)")) +
		       ", SAMPLE_SIZE=" + std::to_string(sample_chunk_size * sample_chunks) +
		       ", ALL_VARCHAR=" + std::to_string(all_varchar);
	}
};

enum class QuoteRule : uint8_t { QUOTES_RFC = 0, QUOTES_OTHER = 1, NO_QUOTES = 2 };

enum class ParserMode : uint8_t { PARSING = 0, SNIFFING_DIALECT = 1, SNIFFING_DATATYPES = 2, PARSING_HEADER = 3 };

static DataChunk DUMMY_CHUNK;

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader {
	//! Initial buffer read size; can be extended for long lines
	static constexpr idx_t INITIAL_BUFFER_SIZE = 16384;
	//! Maximum CSV line size: specified because if we reach this amount, we likely have the wrong delimiters
	static constexpr idx_t MAXIMUM_CSV_LINE_SIZE = 1048576;
	ParserMode mode;

	//! Candidates for delimiter auto detection
	vector<string> delim_candidates = {",", "|", ";", "\t"};
	//! Candidates for quote rule auto detection
	vector<QuoteRule> quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	//! Candidates for quote sign auto detection (per quote rule)
	vector<vector<string>> quote_candidates_map = {{"\""}, {"\"", "'"}, {""}};
	//! Candidates for escape character auto detection (per quote rule)
	vector<vector<string>> escape_candidates_map = {{""}, {"\\"}, {""}};

public:
	BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());

	FileSystem &fs;
	BufferedCSVReaderOptions options;
	vector<LogicalType> sql_types;
	vector<string> col_names;
	unique_ptr<FileHandle> file_handle;
	bool plain_file_source = false;
	idx_t file_size = 0;
	FileCompressionType compression = FileCompressionType::UNCOMPRESSED;

	unique_ptr<char[]> buffer;
	idx_t buffer_size;
	idx_t position;
	idx_t start = 0;

	idx_t linenr = 0;
	bool linenr_estimated = false;

	vector<idx_t> sniffed_column_counts;
	bool row_empty = false;
	idx_t sample_chunk_idx = 0;
	bool jumping_samples = false;
	bool end_of_file_reached = false;
	bool bom_checked = false;

	idx_t bytes_in_chunk = 0;
	double bytes_per_line_avg = 0;

	vector<unique_ptr<char[]>> cached_buffers;

	TextSearchShiftArray delimiter_search, escape_search, quote_search;

	DataChunk parse_chunk;

	std::queue<unique_ptr<DataChunk>> cached_chunks;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Initializes the parse_chunk with varchar columns and aligns info with new number of cols
	void InitParseChunk(idx_t num_cols);
	//! Initializes the TextSearchShiftArrays for complex parser
	void PrepareComplexParser();
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(ParserMode mode, DataChunk &insert_chunk = DUMMY_CHUNK);
	//! Sniffs CSV dialect and determines skip rows, header row, column types and column names
	vector<LogicalType> SniffCSV(const vector<LogicalType> &requested_types);
	//! Change the date format for the type to the string
	void SetDateFormat(const string &format_specifier, const LogicalTypeId &sql_type);
	//! Try to cast a string value to the specified sql type
	bool TryCastValue(const Value &value, const LogicalType &sql_type);
	//! Try to cast a vector of values to the specified sql type
	bool TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type);
	//! Skips skip_rows, reads header row from input stream
	void SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header);
	//! Jumps back to the beginning of input stream and resets necessary internal states
	void JumpToBeginning(idx_t skip_rows, bool skip_header);
	//! Jumps back to the beginning of input stream and resets necessary internal states
	bool JumpToNextSample();
	//! Resets the buffer
	void ResetBuffer();
	//! Resets the steam
	void ResetStream();
	//! Prepare candidate sets for auto detection based on user input
	void PrepareCandidateSets();

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

	unique_ptr<FileHandle> OpenCSV(ClientContext &context, const BufferedCSVReaderOptions &options);
};

} // namespace duckdb
