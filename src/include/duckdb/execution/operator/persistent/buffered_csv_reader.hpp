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
#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/operator/persistent/csv_file_handle.hpp"
#include "duckdb/execution/operator/persistent/csv_buffer.hpp"

#include <sstream>

namespace duckdb {
struct CopyInfo;
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

enum class ParserMode : uint8_t { PARSING = 0, SNIFFING_DIALECT = 1, SNIFFING_DATATYPES = 2, PARSING_HEADER = 3 };

struct ReadCSVLocalState;

struct CSVBufferRead {
	CSVBufferRead(shared_ptr<CSVBuffer> buffer_p, idx_t buffer_start_p, idx_t buffer_end_p)
	    : buffer(move(buffer_p)), buffer_start(buffer_start_p), buffer_end(buffer_end_p) {
		if (buffer) {
			if (buffer_end > buffer->GetBufferSize()) {
				buffer_end = buffer->GetBufferSize();
			}
		}
	}

	CSVBufferRead() : buffer_start(0), buffer_end(NumericLimits<idx_t>::Maximum()) {};

	void Reset() {
		buffer.reset();
		buffer_start = 0;
		buffer_end = NumericLimits<idx_t>::Maximum();
	}

	shared_ptr<CSVBuffer> buffer;

	idx_t buffer_start;
	idx_t buffer_end;
};

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader {
	ParserMode mode;

public:
	BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options, CSVFileHandle *file_handle,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());

	BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options, const CSVBufferRead &buffer,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	~BufferedCSVReader();

	Allocator &allocator;
	CSVFileHandle *file_handle;
	BufferedCSVReaderOptions options;
	vector<LogicalType> sql_types;
	vector<string> col_names;

	//! remap parse_chunk col to insert_chunk col, because when
	//! union_by_name option on insert_chunk may have more cols
	vector<idx_t> insert_cols_idx;
	vector<idx_t> insert_nulls_idx;

	//! Current Position (Relative to the Buffer)
	idx_t position_buffer = 0;

	//! Start of the piece of the buffer this thread should read
	idx_t start_buffer = 0;
	//! End of the piece of this buffer this thread should read
	idx_t end_buffer = NumericLimits<idx_t>::Maximum();
	//! The actual buffer size
	idx_t buffer_size = 0;

	idx_t bytes_per_thread = 0;

	char *buffer;

	CSVBufferRead buffer_read;

	idx_t linenr = 0;
	bool linenr_estimated = false;

	vector<idx_t> sniffed_column_counts;
	bool row_empty = false;
	idx_t sample_chunk_idx = 0;
	bool jumping_samples = false;
	bool end_of_file_reached = false;

	idx_t bytes_in_chunk = 0;
	double bytes_per_line_avg = 0;

	vector<unique_ptr<char[]>> cached_buffers;

	TextSearchShiftArray delimiter_search, escape_search, quote_search;

	DataChunk parse_chunk;

	std::queue<unique_ptr<DataChunk>> cached_chunks;

public:
	void SetBufferRead(const CSVBufferRead &buffer_read);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);

	//! Fill nulls into the cols that mismtach union names
	void SetNullUnionCols(DataChunk &insert_chunk);

	//! Resets the buffer
	void ResetBuffer();

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Initializes the parse_chunk with varchar columns and aligns info with new number of cols
	void InitParseChunk(idx_t num_cols);
	//! Initializes the insert_chunk idx for mapping parse_chunk cols to insert_chunk cols
	void InitInsertChunkIdx(idx_t num_cols);
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
	//! Resets the steam
	void ResetStream();
	//! Sets Position depending on the byte_start of this thread
	bool SetPosition();
	//! When a buffer finishes reading its piece, it still can try to scan up to the real end of the buffer
	//! Up to finding a new line. This function sets the buffer_end and marks a boolean variable
	//! when changing the buffer end the first time.
	//! It returns FALSE if the parser should jump to the final state of parsing or not
	bool BufferRemainder(bool &reached_remainder_state);
	//! Parses a CSV file with a one-byte delimiter, escape and quote character
	bool TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message);
	//! Parses more complex CSV files with multi-byte delimiters, escapes or quotes
	//	bool TryParseComplexCSV(DataChunk &insert_chunk, string &error_message, RemainderLines *remainder_lines);

	//! Adds a value to the current row
	void AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions, bool has_quotes);
	//! Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row being added
	bool AddRow(DataChunk &insert_chunk, idx_t &column);

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
	//! Finalizes a chunk, parsing all values that have been added so far and adding them to the insert_chunk
	void Flush(DataChunk &insert_chunk);

private:
	//! Whether or not the current row's columns have overflown sql_types.size()
	bool error_column_overflow = false;
};

} // namespace duckdb
