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
#include "duckdb/common/map.hpp"
#include "duckdb/common/queue.hpp"

#include <sstream>

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

struct BufferedCSVReaderOptions {
	//===--------------------------------------------------------------------===//
	// CommonCSVOptions
	//===--------------------------------------------------------------------===//

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
	//! Whether or not we should ignore InvalidInput errors
	bool ignore_errors = false;
	//! Expected number of columns
	idx_t num_cols = 0;
	//! Number of samples to buffer
	idx_t buffer_size = STANDARD_VECTOR_SIZE * 100;
	//! Specifies the string that represents a null value
	string null_str;
	//! Whether file is compressed or not, and if so which compression type
	//! AUTO_DETECT (default; infer from file extension)
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! The column names of the columns to read/write
	vector<string> names;

	//===--------------------------------------------------------------------===//
	// ReadCSVOptions
	//===--------------------------------------------------------------------===//

	//! How many leading rows to skip
	idx_t skip_rows = 0;
	//! Maximum CSV line size: specified because if we reach this amount, we likely have wrong delimiters (default: 2MB)
	//! note that this is the guaranteed line length that will succeed, longer lines may be accepted if slightly above
	idx_t maximum_line_size = 2097152;
	//! Whether or not header names shall be normalized
	bool normalize_names = false;
	//! True, if column with that index must skip null check
	vector<bool> force_not_null;
	//! Consider all columns to be of type varchar
	bool all_varchar = false;
	//! Size of sample chunk used for dialect and type detection
	idx_t sample_chunk_size = STANDARD_VECTOR_SIZE;
	//! Number of sample chunks used for type detection
	idx_t sample_chunks = 10;
	//! Whether or not to automatically detect dialect and datatypes
	bool auto_detect = false;
	//! The file path of the CSV file to read
	string file_path;
	//! Whether or not to include a file name column
	bool include_file_name = false;
	//! Whether or not to include a parsed hive partition columns
	bool include_parsed_hive_partitions = false;

	//===--------------------------------------------------------------------===//
	// WriteCSVOptions
	//===--------------------------------------------------------------------===//

	//! True, if column with that index must be quoted
	vector<bool> force_quote;

	//! The date format to use (if any is specified)
	std::map<LogicalTypeId, StrpTimeFormat> date_format = {{LogicalTypeId::DATE, {}}, {LogicalTypeId::TIMESTAMP, {}}};
	//! The date format to use for writing (if any is specified)
	std::map<LogicalTypeId, StrfTimeFormat> write_date_format = {{LogicalTypeId::DATE, {}},
	                                                             {LogicalTypeId::TIMESTAMP, {}}};
	//! Whether or not a type format is specified
	std::map<LogicalTypeId, bool> has_format = {{LogicalTypeId::DATE, false}, {LogicalTypeId::TIMESTAMP, false}};

	void SetDelimiter(const string &delimiter);
	//! Set an option that is supported by both reading and writing functions, called by
	//! the SetReadOption and SetWriteOption methods
	bool SetBaseOption(const string &loption, const Value &value);

	//! loption - lowercase string
	//! set - argument(s) to the option
	//! expected_names - names expected if the option is "columns"
	void SetReadOption(const string &loption, const Value &value, vector<string> &expected_names);

	void SetWriteOption(const string &loption, const Value &value);
	void SetDateFormat(LogicalTypeId type, const string &format, bool read_format);

	std::string ToString() const;
};

enum class ParserMode : uint8_t { PARSING = 0, SNIFFING_DIALECT = 1, SNIFFING_DATATYPES = 2, PARSING_HEADER = 3 };

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader {
	//! Initial buffer read size; can be extended for long lines
	static constexpr idx_t INITIAL_BUFFER_SIZE = 16384;
	//! Larger buffer size for non disk files
	static constexpr idx_t INITIAL_BUFFER_SIZE_LARGE = 10000000; // 10MB
	ParserMode mode;

public:
	BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());

	BufferedCSVReader(FileSystem &fs, Allocator &allocator, FileOpener *opener, BufferedCSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	~BufferedCSVReader();

	FileSystem &fs;
	Allocator &allocator;
	FileOpener *opener;
	BufferedCSVReaderOptions options;
	vector<LogicalType> sql_types;
	vector<string> col_names;
	unique_ptr<CSVFileHandle> file_handle;

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

	idx_t GetFileSize();

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Initializes the parse_chunk with varchar columns and aligns info with new number of cols
	void InitParseChunk(idx_t num_cols);
	//! Initializes the TextSearchShiftArrays for complex parser
	void PrepareComplexParser();
	//! Try to parse a single datachunk from the file. Throws an exception if anything goes wrong.
	void ParseCSV(ParserMode mode);
	//! Try to parse a single datachunk from the file. Returns whether or not the parsing is successful
	bool TryParseCSV(ParserMode mode);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	bool TryParseCSV(ParserMode mode, DataChunk &insert_chunk, string &error_message);
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

	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message);
	//! Parses more complex CSV files with multi-byte delimiters, escapes or quotes
	bool TryParseComplexCSV(DataChunk &insert_chunk, string &error_message);

	//! Adds a value to the current row
	void AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions);
	//! Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row being added
	bool AddRow(DataChunk &insert_chunk, idx_t &column);
	//! Finalizes a chunk, parsing all values that have been added so far and adding them to the insert_chunk
	void Flush(DataChunk &insert_chunk);
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(idx_t &start);

	unique_ptr<CSVFileHandle> OpenCSV(const BufferedCSVReaderOptions &options);

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

private:
	//! Whether or not the current row's columns have overflown sql_types.size()
	bool error_column_overflow = false;
};

} // namespace duckdb
