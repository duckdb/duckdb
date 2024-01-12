//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/util/csv_error.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/execution/operator/csv_scanner/options/csv_reader_options.hpp"

namespace duckdb {

//! Object that holds information on how many lines each csv batch read.

class LinesPerBoundary {
public:
	LinesPerBoundary();
	LinesPerBoundary(idx_t boundary_idx, idx_t lines_in_batch);

	idx_t boundary_idx = 0;
	idx_t lines_in_batch = 0;
};

enum CSVErrorType : uint8_t {
	CAST_ERROR = 0,                // If when casting a value from string to the column type fails
	COLUMN_NAME_TYPE_MISMATCH = 1, // If there is a mismatch between Column Names and Types
	INCORRECT_COLUMN_AMOUNT = 2,   // If the CSV is missing a column
	UNTERMINATED_QUOTES = 3,       // If a quote is not terminated
	SNIFFING = 4,     // If something went wrong during sniffing and was not possible to find suitable candidates
	MAXIMUM_LINE_SIZE // Maximum line size was exceeded by a line in the CSV File
};

class CSVError {
public:
	CSVError(string error_message, CSVErrorType type, idx_t column_idx, vector<Value> row);
	CSVError(string error_message, CSVErrorType type);
	//! Produces error messages for column name -> type mismatch.
	static CSVError ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);
	//! Produces error messages for casting errors
	static CSVError CastError(const CSVReaderOptions &options, DataChunk &parse_chunk, idx_t chunk_row,
	                          string &column_name, string &cast_error, idx_t &column_idx, vector<Value> &row);
	//! Produces error for when the line size exceeds the maximum line size option
	static CSVError LineSizeError(const CSVReaderOptions &options, idx_t actual_size);
	//! Produces error for when the sniffer couldn't find viable options
	static CSVError SniffingError(string &file_path);
	//! Produces error messages for unterminated quoted values
	static CSVError UnterminatedQuotesError(const CSVReaderOptions &options, string_t *vector_ptr,
	                                        idx_t vector_line_start, idx_t current_column);
	//! Produces error for incorrect (e.g., smaller and lower than the predefined) number of columns in a CSV Line
	static CSVError IncorrectColumnAmountError(const CSVReaderOptions &options, string_t *vector_ptr,
	                                           idx_t vector_line_start, idx_t actual_columns);
	//! Actual error message
	string error_message;
	//! Error Type
	CSVErrorType type;
	//! Column Index where error happened
	idx_t column_idx;
	//! Values from the row where error happened
	vector<Value> row;
};

class CSVErrorHandler {
public:
	CSVErrorHandler(bool ignore_errors = false);
	//! Throws the error
	void Error(LinesPerBoundary &error_info, CSVError &csv_error, bool force_error = false);
	//! Throws the error
	void Error(CSVError &csv_error);
	//! Inserts a finished error info
	void Insert(idx_t boundary_idx, idx_t rows);
	vector<pair<LinesPerBoundary, CSVError>> errors;
	//! Return the 1-indexed line number
	idx_t GetLine(LinesPerBoundary &error_info);
	void NewMaxLineSize(idx_t scan_line_size);
	idx_t GetMaxLineLength() {
		return max_line_length;
	}

private:
	//! If we should print the line of an error
	bool PrintLineNumber(CSVError &error);
	//! CSV Error Handler Mutex
	mutex main_mutex;
	//! Map of <file,batch> -> lines
	unordered_map<idx_t, LinesPerBoundary> lines_per_batch_map;
	idx_t max_line_length = 0;
	bool ignore_errors = false;
};

} // namespace duckdb
