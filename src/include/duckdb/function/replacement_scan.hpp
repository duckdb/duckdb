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

class ClientContext;
class TableFunctionRef;

struct ReplacementScanData {
	virtual ~ReplacementScanData() {
	}
};

typedef unique_ptr<TableFunctionRef> (*replacement_scan_t)(ClientContext &context, const string &table_name,
                                                           ReplacementScanData *data);

//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
//! This allows you to do e.g. SELECT * FROM 'filename.csv', and automatically convert this into a CSV scan
struct ReplacementScan {
	explicit ReplacementScan(replacement_scan_t function, unique_ptr<ReplacementScanData> data_p = nullptr)
	    : function(function), data(std::move(data_p)) {
	}

	replacement_scan_t function;
	unique_ptr<ReplacementScanData> data;
};

} // namespace duckdb
