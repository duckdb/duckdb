//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/scan_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class InsertedScanType {
	// get all rows valid for the transaction
	STANDARD,
	// scan all rows including transaction local rows
	ALL_ROWS
};
enum class DeletedScanType {
	//! omit any rows that are deleted
	STANDARD,
	//! include all rows, including all deleted rows
	INCLUDE_ALL_DELETED,
	//! omit committed deleted rows
	OMIT_COMMITTED_DELETES,
	//! omit deleted rows that have been fully deleted - i.e. no active transaction still depends on them
	OMIT_FULLY_COMMITTED_DELETES
};

enum class UpdateScanType {
	//! allow updates
	STANDARD,
	// disallow updates - throw on updates
	DISALLOW_UPDATES
};

struct TScanType {
	InsertedScanType insert_type = InsertedScanType::STANDARD;
	DeletedScanType delete_type = DeletedScanType::STANDARD;
	UpdateScanType update_type = UpdateScanType::STANDARD;
};

enum class TableScanType {
	//! Regular table scan: scan all tuples that are relevant for the current transaction
	TABLE_SCAN_REGULAR = 0,
	//! Scan all rows, including any deleted rows. Committed updates are merged in.
	TABLE_SCAN_COMMITTED_ROWS = 1,
	//! Scan all rows, excluding any permanently deleted rows.
	//! Permanently deleted rows are rows which no transaction will ever need again.
	TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED = 3,
	//! Scan the latest committed rows
	TABLE_SCAN_LATEST_COMMITTED_ROWS = 4
};

} // namespace duckdb
