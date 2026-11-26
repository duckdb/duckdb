//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/table_function_query_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;
class Vector;

//! Row filter described by the open options of a file in a file list:
//!  filter_query  SQL query that returns the rows of the file to scan as (row_start BIGINT, row_end BIGINT, bits BIT
//!                [, row_value BIGINT]). Every row of the result describes some rows, and the rows to scan are all of
//!                them together:
//!                - bits is NULL: the rows from row_start up to but not including row_end. A NULL row_start is the
//!                  start of the file, a NULL row_end is the end of the file
//!                - bits is set: a bitmap window - bit i is the row with id row_start + i (row_end is not used). A NULL
//!                  row_start is the start of the file
//!                A row_value that is set is the value of the rows of that result row - a reader can ask for the value
//!                of the rows it scans (GatherRowValues). The rows with a value should not overlap
class TableFunctionQueryFilter : public DeleteFilter {
public:
	//! Create the filter for a file - returns nullptr if its options do not have a filter query
	static unique_ptr<DeleteFilter> TryCreate(ClientContext &context, const OpenFileInfo &file);
	//! Whether the options of a file have a filter query - the rows of such a file cannot be counted from its metadata
	static bool HasFilterQuery(const OpenFileInfo &file);

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;
	bool HasRowValues() const override;
	void GatherRowValues(const Vector &file_row_numbers, Vector &result, idx_t count) override;

private:
	//! The rows from start up to but not including end have a value
	struct ScanValue {
		idx_t start;
		idx_t end;
		int64_t value;
	};
	//! 64 rows of a bitmap window: bit i of the word is the row 64 * index + i
	struct ScanWord {
		idx_t index;
		uint64_t bits;
	};

private:
	void ReadFilterQuery(ClientContext &context, const string &query, const string &path);
	void AddRows(DataChunk &chunk, const string &path);
	void AddValue(idx_t row_start, idx_t row_end, int64_t value);
	void AddWindow(idx_t window_start, const char *data, idx_t size);
	void AddWindowBits(idx_t row_start, uint64_t bits, idx_t bit_count);
	//! Sort the ranges and words, and merge those that overlap or touch
	void Finalize();

private:
	//! The rows to scan as ranges, sorted and not overlapping or touching: from the first row up to but not including
	//! the second
	vector<pair<idx_t, idx_t>> scan_ranges;
	//! The rows to scan as bitmap windows, as sorted words that are not empty
	vector<ScanWord> scan_words;
	//! The values of the rows, sorted
	vector<ScanValue> scan_values;
};

} // namespace duckdb
