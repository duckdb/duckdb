#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

struct PythonReplacementScan {
public:
	static unique_ptr<TableRef> Replace(ClientContext &context, ReplacementScanInput &input,
	                                    optional_ptr<ReplacementScanData> data);
	static unique_ptr<TableRef> TryReplacementObject(const py::object &entry, const string &name,
	                                                 ClientContext &context);
};

} // namespace duckdb
