//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/buffered_csv_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/base_csv_reader.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {
struct CopyInfo;
struct CSVFileHandle;
struct FileHandle;
struct StrpTimeFormat;

class FileOpener;
class FileSystem;

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader : public BaseCSVReader {
	//! Initial buffer read size; can be extended for long lines
	static constexpr idx_t INITIAL_BUFFER_SIZE = 16384;
	//! Larger buffer size for non disk files
	static constexpr idx_t INITIAL_BUFFER_SIZE_LARGE = 10000000; // 10MB

public:
	BufferedCSVReader(ClientContext &context, CSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	BufferedCSVReader(ClientContext &context, string filename, CSVReaderOptions options,
	                  const vector<LogicalType> &requested_types = vector<LogicalType>());
	virtual ~BufferedCSVReader() {
	}

	unsafe_unique_array<char> buffer;
	idx_t buffer_size;
	idx_t position;
	idx_t start = 0;

	vector<unsafe_unique_array<char>> cached_buffers;

	unique_ptr<CSVFileHandle> file_handle;
	//! CSV State Machine Cache
	CSVStateMachineCache state_machine_cache;

public:
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	void ParseCSV(DataChunk &insert_chunk);
	static string ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);

private:
	//! Initialize Parser
	void Initialize(const vector<LogicalType> &requested_types);
	//! Skips skip_rows, reads header row from input stream
	void SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header);
	//! Resets the buffer
	void ResetBuffer();
	//! Reads a new buffer from the CSV file if the current one has been exhausted
	bool ReadBuffer(idx_t &start, idx_t &line_start);
	//! Try to parse a single datachunk from the file. Throws an exception if anything goes wrong.
	void ParseCSV(ParserMode mode);
	//! Extract a single DataChunk from the CSV file and stores it in insert_chunk
	bool TryParseCSV(ParserMode mode, DataChunk &insert_chunk, string &error_message);
	//! Skip Empty lines for tables with over one column
	void SkipEmptyLines();
};

} // namespace duckdb
