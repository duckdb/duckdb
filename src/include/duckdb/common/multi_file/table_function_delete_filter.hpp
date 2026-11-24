//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/table_function_delete_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {
class ClientContext;

//! Delete filter described by the open options of a file in a file list:
//!  delete_query  SQL query that returns the deleted row positions of the file in a "pos" column
//!  scan_rows     LIST of row positions - only these rows are produced (the inverse of the deletes)
//!  max_row_count rows at or past this position are not part of the file
class TableFunctionDeleteFilter : public DeleteFilter {
public:
	//! Create the filter for a file - returns nullptr if its options do not describe any deletes
	static unique_ptr<DeleteFilter> TryCreate(ClientContext &context, const OpenFileInfo &file);
	//! Whether the options of a file describe deletes - the rows of such a file cannot be counted from its metadata
	static bool HasDeleteOptions(const OpenFileInfo &file);

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

private:
	void AddDeletes(ClientContext &context, const string &query, const string &path);

private:
	//! Sorted, distinct positions of the deleted rows
	vector<idx_t> deleted_rows;
	//! When set, only these (sorted) positions are produced
	vector<idx_t> scan_rows;
	bool has_scan_rows = false;
	optional_idx max_row_count;
};

} // namespace duckdb
