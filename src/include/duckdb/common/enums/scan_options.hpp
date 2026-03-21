//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/scan_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_data.hpp"

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
	//! omit only deleted rows that have been committed and have a commit ts lower than the transaction start
	OMIT_COMMITTED_DELETES
};

enum class UpdateScanType {
	//! allow updates
	STANDARD,
	// disallow updates - throw on updates
	DISALLOW_UPDATES
};

struct ScanOptions {
	ScanOptions(TransactionData transaction); // NOLINT: allow implicit conversion from transaction

	TransactionData transaction;
	InsertedScanType insert_type = InsertedScanType::STANDARD;
	DeletedScanType delete_type = DeletedScanType::STANDARD;
	UpdateScanType update_type = UpdateScanType::STANDARD;
};

enum class TableScanType {
	//! Scan all rows, including any deleted rows. Committed updates are merged in.
	TABLE_SCAN_ALL_ROWS = 1,
	//! Scan all rows, excluding any permanently deleted rows.
	//! Permanently deleted rows are rows which no transaction will ever need again.
	TABLE_SCAN_OMIT_PERMANENTLY_DELETED = 2,
	//! Scan the latest committed rows
	TABLE_SCAN_COMMITTED_ROWS = 3
};

} // namespace duckdb
