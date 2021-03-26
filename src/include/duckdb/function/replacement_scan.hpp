//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/replacement_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class TableFunctionRef;

typedef unique_ptr<TableFunctionRef> (*replacement_scan_t)(const string &table_name, void *data);

//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
//! This allows you to do e.g. SELECT * FROM 'filename.csv', and automatically convert this into a CSV scan
struct ReplacementScan {
	explicit ReplacementScan(replacement_scan_t function, void *data = nullptr) : function(function), data(data) {
	}

	replacement_scan_t function;
	void *data;
};

} // namespace duckdb
