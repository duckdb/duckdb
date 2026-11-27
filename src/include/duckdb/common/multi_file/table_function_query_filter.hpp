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
class BaseFileReader;
class ClientContext;
class DataChunk;

//! Row filter described by the open options of a file in a file list:
//!  filter_query  SQL query that returns the rows of the file to scan as (row_start BIGINT, row_end BIGINT, bits BIT).
//!                Every row of the result describes some rows, and the rows to scan are all of them together:
//!                - bits is NULL: the rows from row_start up to but not including row_end. A NULL row_start is the
//!                  start of the file, a NULL row_end is the end of the file
//!                - bits is set: a bitmap window - bit i is the row with id row_start + i (row_end is not used). A NULL
//!                  row_start is the start of the file
class TableFunctionQueryFilter : public DeleteFilter {
public:
	//! Create the filter for the file of a reader - returns nullptr if its options do not have a filter query. The
	//! query runs in the transaction of the context, and must be a single SELECT statement that does not modify a
	//! database
	static unique_ptr<DeleteFilter> TryCreate(ClientContext &context, BaseFileReader &reader);
	//! The open option that holds the filter query
	static constexpr const char *FILTER_QUERY_OPTION = "filter_query";

	//! Whether the options of a file have a filter query - the rows of such a file cannot be counted from its metadata.
	//! Inline: extensions call it without linking the query that TryCreate runs
	static bool HasFilterQuery(const OpenFileInfo &file) {
		if (!file.extended_info) {
			return false;
		}
		auto entry = file.extended_info->options.find(FILTER_QUERY_OPTION);
		return entry != file.extended_info->options.end() && !entry->second.IsNull();
	}

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

private:
	//! 64 rows of a bitmap window: bit i of the word is the row 64 * index + i
	struct ScanWord {
		idx_t index;
		uint64_t bits;
	};

private:
	void ReadFilterQuery(ClientContext &context, const string &query, const string &path);
	void AddRows(DataChunk &chunk);
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
};

} // namespace duckdb
