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
class Vector;

//! Row filter described by the open options of a file in a file list:
//!  filter_query  SQL query that describes the rows of the file to scan. What it returns is read by its type:
//!                - integers: the row ids of the rows to scan (any order, duplicates, NULLs and ids outside the file
//!                  are ignored). The rows of the file are the rows of the CTE file_row_ids(row_id)
//!                - a BIT value: a bitmap of the rows to scan, bit i being the row with id i. Rows past the end of the
//!                  bitmap are not scanned
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
	void AddRowIds(Vector &row_ids, const string &path);
	void AddBitmap(const char *data, idx_t size, const string &path);
	bool RowIsScanned(idx_t row_idx) const;

private:
	//! The rows to scan, as the bytes of a BIT value: the number of padding bits, then the bits (the first of the
	//! rows in the highest bit of a byte)
	string scan_bits;
	//! The number of rows in the bitmap
	idx_t bit_count = 0;
};

} // namespace duckdb
