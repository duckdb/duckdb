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

namespace duckdb {

//! Object that holds information on how many lines each csv batch read.

struct BatchInfo {
	idx_t file_idx;
	idx_t batch_idx;
	bool operator==(const BatchInfo &other) const {
		return file_idx == other.file_idx && batch_idx == other.batch_idx;
	}
};

class LinesPerBatch {
public:
	LinesPerBatch() : initialized(false) {};
	LinesPerBatch(idx_t file_idx, idx_t batch_idx, idx_t lines_in_batch) {};
	LinesPerBatch(LinesPerBatch const &line_info) {};

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

class CSVErrorHandler {
public:
	CSVErrorHandler() {};
	//! Throws the error
	void Error(LinesPerBatch &error_info, string &error_message);
	//! Inserts a finished error info
	void Insert(LinesPerBatch &error_info);

private:
	//! Return the 1-indexed line number
	idx_t GetLine(LinesPerBatch &error_info);

	//! CSV Error Handler Mutex
	mutex main_mutex;
	//! Map of <file,batch> -> lines
	unordered_map<BatchInfo, LinesPerBatch, HashCSVBatchInfo> lines_per_batch_map;
};

} // namespace duckdb
