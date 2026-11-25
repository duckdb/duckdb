//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/table_function_row_id_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {
class ClientContext;

//! Row filter described by the open options of a file in a file list:
//!  row_id_query  SQL query that returns the row ids of the file to scan in a "row_id" column. The rows of the file
//!                are the rows of the view __file_row_ids(row_id), so the query decides which of them are produced
class TableFunctionRowIdFilter : public DeleteFilter {
public:
	//! Create the filter for a file - returns nullptr if its options do not have a row id query
	static unique_ptr<DeleteFilter> TryCreate(ClientContext &context, BaseFileReader &reader);
	//! Whether the options of a file have a row id query - the rows of such a file cannot be counted from its metadata
	static bool HasRowIdQuery(const OpenFileInfo &file);

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

private:
	void ReadRowIds(ClientContext &context, const string &query, const string &path);

private:
	//! The rows of the file
	idx_t row_count = 0;
	//! The rows to scan
	ValidityMask scan_rows;
};

} // namespace duckdb
