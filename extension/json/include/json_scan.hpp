//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "buffered_json_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "json_common.hpp"

namespace duckdb {

struct JSONScanData : public TableFunctionData {
	//! The file path of the JSON files to read
	vector<string> files;
	//! The JSON reader options
	BufferedJSONReaderOptions options;

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
};

struct JSONScanGlobalState : public GlobalTableFunctionState {
	//! The JSON reader
	unique_ptr<BufferedJSONReader> json_reader;
	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index;

	//! Current and previous buffer
	AllocatedData current_buffer;
	AllocatedData previous_buffer;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
};

struct JSONScanLocalState : public LocalTableFunctionState {
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state);
};

} // namespace duckdb
