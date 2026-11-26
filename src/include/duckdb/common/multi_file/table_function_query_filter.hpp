//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/table_function_query_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {
class ClientContext;
class DataChunk;
class Vector;

//! Row filter described by the open options of a file in a file list:
//!  filter_query  SQL query that describes the rows of the file to scan. What it returns is read by its type:
//!                - integers: the row ids of the rows to scan (any order, duplicates, NULLs and ids outside the file
//!                  are ignored). The rows of the file are the rows of the CTE file_row_ids(row_id)
//!                - a BIT value: a bitmap of the rows to scan, bit i being the row with id i. Rows past the end of the
//!                  bitmap are not scanned
//!                - two BIGINT columns (row_start, row_end): the ranges of rows to scan, from row_start up to but not
//!                  including row_end. A NULL row_start is the start of the file, a NULL row_end is the end of the file
class TableFunctionQueryFilter : public DeleteFilter {
public:
	//! Create the filter for a file - returns nullptr if its options do not have a filter query
	static unique_ptr<DeleteFilter> TryCreate(ClientContext &context, BaseFileReader &reader);
	//! Whether the options of a file have a filter query - the rows of such a file cannot be counted from its metadata
	static bool HasFilterQuery(const OpenFileInfo &file);

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

private:
	void ReadFilterQuery(ClientContext &context, const string &query, const string &path, optional_idx row_count);
	template <class T>
	void AddRowIds(Vector &row_ids);
	void AddBitmap(const char *data, idx_t size, const string &path);
	void AddRanges(DataChunk &chunk);
	//! Sort the ranges and merge those that overlap or touch
	void FinalizeRanges();
	bool RowIsScanned(idx_t row_idx) const;
	//! Whether all of the rows are scanned (only rows within the bitmap)
	bool RowsAreScanned(idx_t row_start, idx_t count) const;

private:
	//! Whether the rows to scan are the ranges (otherwise the bitmap)
	bool has_ranges = false;
	//! The rows to scan, as the bytes of a BIT value: the number of padding bits, then the bits (the first of the
	//! rows in the highest bit of a byte)
	string scan_bits;
	//! The number of rows in the bitmap
	idx_t bit_count = 0;
	//! The rows to scan, as sorted ranges that do not overlap or touch: from the first row up to but not including
	//! the second
	vector<pair<idx_t, idx_t>> scan_ranges;
};

} // namespace duckdb
