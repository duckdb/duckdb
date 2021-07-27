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

enum class TableScanType : uint8_t {
	//! Regular table scan: scan all tuples that are relevant for the current transaction
	TABLE_SCAN_REGULAR = 0,
	//! Scan all rows, including any deleted rows. Committed updates are merged in.
	TABLE_SCAN_COMMITTED_ROWS = 1,
	//! Scan all rows, including any deleted rows. Throws an exception if there are any uncommitted updates.
	TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES = 2,
	//! Scan all rows, excluding any permanently deleted rows.
	//! Permanently deleted rows are rows which no transaction will ever need again.
	TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED = 3
};

} // namespace duckdb
