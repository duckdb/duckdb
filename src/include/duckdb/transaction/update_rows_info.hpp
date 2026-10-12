//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/update_rows_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class RowVersionManager;

//! The undo entry of a transaction that updates rows of a row group (see RowVersionManager::UpdateRows)
struct UpdateRowsInfo {
	RowVersionManager *version_info;
	//! The commit id of the writer, to revert the commit
	transaction_t commit_id;
};

} // namespace duckdb
