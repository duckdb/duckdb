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
	//! only omit permanently committed deleted rows (i.e. rows that no transaction still depends on)
	OMIT_PERMANENTLY_COMMITTED
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
	static TScanType IndexScan() {
		TScanType type;
		type.insert_type = InsertedScanType::ALL_ROWS;
		type.delete_type = DeletedScanType::OMIT_PERMANENTLY_COMMITTED;
		type.update_type = UpdateScanType::DISALLOW_UPDATES;
		return type;
	}
};

enum class TableScanType : uint8_t {
	//! Regular table scan: scan all tuples that are relevant for the current transaction
	TABLE_SCAN_REGULAR = 0,
	//! Scan all rows, including any deleted rows. Committed updates are merged in.
	TABLE_SCAN_COMMITTED_ROWS = 1,
	//! Scan all rows, including any deleted rows. Throws an exception if there are any uncommitted updates.
	TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES = 2,
	//! Scan all rows, excluding any permanently deleted rows.
	//! Permanently deleted rows are rows which no transaction will ever need again.
	TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED = 3,
	//! Scan the latest committed rows
	TABLE_SCAN_LATEST_COMMITTED_ROWS = 4
};

} // namespace duckdb
