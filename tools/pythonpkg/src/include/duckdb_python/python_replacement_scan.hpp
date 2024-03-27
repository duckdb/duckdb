#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/function/replacement_scan.hpp"

namespace duckdb {

struct PythonReplacementScan {
public:
	static unique_ptr<TableRef> Replace(ClientContext &context, const string &table_name, ReplacementScanData *data);
};

} // namespace duckdb
