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

namespace duckdb {

//! Object that holds information on how many lines each csv batch read.

struct BatchInfo {
	BatchInfo();
	BatchInfo(idx_t file_idx, idx_t batch_idx);
	idx_t file_idx;
	idx_t batch_idx;
	bool operator==(const BatchInfo &other) const {
		return file_idx == other.file_idx && batch_idx == other.batch_idx;
	}
};

class LinesPerBatch {
public:
	LinesPerBatch();
	LinesPerBatch(idx_t file_idx, idx_t batch_idx, idx_t lines_in_batch);

	BatchInfo batch_info;
	idx_t lines_in_batch;
	bool initialized;
};

//! Hash function used in to hash the Lines Per Batch Information
struct HashCSVBatchInfo {
	size_t operator()(BatchInfo const &batch_info) const noexcept {
		return CombineHash(batch_info.file_idx, batch_info.batch_idx);
	}
};

enum CSVErrorType : uint8_t {
	CAST_ERROR = 0,                // If when casting a value from string to the column type fails
	COLUMN_NAME_TYPE_MISMATCH = 1, // If there is a mismatch between Column Names and Types
	MISSING_COLUMNS = 2,           // If the CSV is missing a column
	TOO_MANY_COLUMNS = 3,          // If the CSV file has too many columns
	UNTERMINATED_QUOTES = 4        // If a quote is not terminated
};

class CSVError {
public:
	CSVError(string error_mesasge, CSVErrorType type);
	//! Produces error messages for column name -> type mismatch.
	static CSVError ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column, const vector<string> &names);
	//! Produces error messages for casting errors
	static CSVError CastError(string &column_name, string &cast_error);

	string error_message;
	CSVErrorType type;
};

class CSVErrorHandler {
public:
	CSVErrorHandler(bool ignore_errors = false);
	//! Throws the error
	void Error(LinesPerBatch &error_info, CSVError &error_message);
	//! Inserts a finished error info
	void Insert(LinesPerBatch &error_info);

private:
	//! Return the 1-indexed line number
	idx_t GetLine(LinesPerBatch &error_info);
	//! CSV Error Handler Mutex
	mutex main_mutex;
	//! Map of <file,batch> -> lines
	unordered_map<BatchInfo, LinesPerBatch, HashCSVBatchInfo> lines_per_batch_map;
	bool ignore_errors = false;
};

} // namespace duckdb
