//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/options_delete_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {
class ClientContext;

//! Delete filter described by the open options of a file in a file list:
//!  deleted_rows           LIST of row positions that are deleted from the file
//!  delete_file            file (path or file struct) of a parquet file with a "pos" column (and optionally a
//!                         "_ducklake_internal_snapshot_id" column) that lists more deleted positions
//!  delete_file_function   table function that reads the delete file (default read_parquet): (pos, snapshot id)
//!  delete_snapshot_filter only deletions with a snapshot id <= this value apply (delete_file with snapshot ids)
//!  scan_rows              LIST of row positions - only these rows are produced (the inverse of deleted_rows)
//!  max_row_count          rows at or past this position are not part of the file
class OptionsDeleteFilter : public DeleteFilter {
public:
	//! Create the filter for a file - returns nullptr if its options do not describe any deletes
	static unique_ptr<DeleteFilter> TryCreate(ClientContext &context, const OpenFileInfo &file);
	//! Whether the options of a file describe deletes - the rows of such a file cannot be counted from its metadata
	static bool HasDeleteOptions(const OpenFileInfo &file);

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

private:
	void AddDeleteFile(ClientContext &context, const Value &delete_file, const string &function_name);

private:
	//! Sorted positions of the deleted rows, and (optionally) the snapshot that deleted each of them
	vector<idx_t> deleted_rows;
	vector<idx_t> snapshot_ids;
	//! When set, only these (sorted) positions are produced
	vector<idx_t> scan_rows;
	bool has_scan_rows = false;
	optional_idx snapshot_filter;
	optional_idx max_row_count;
};

} // namespace duckdb
