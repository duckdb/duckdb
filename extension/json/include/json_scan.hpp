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

struct JSONBufferHandle {
	idx_t readers = 0;
	AllocatedData buffer;
};

struct JSONScanData : public TableFunctionData {
	//! The file path of the JSON files to read
	vector<string> files;
	//! The JSON reader options
	BufferedJSONReaderOptions options;

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);
};

struct JSONScanGlobalState : public GlobalTableFunctionState {
public:
	//! The JSON reader
	unique_ptr<BufferedJSONReader> json_reader;
	//! Next buffer index
	idx_t buffer_index = 0;
	//! Currently pinned buffers
	unordered_map<idx_t, JSONBufferHandle> buffers;
	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index;

public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
};

struct JSONScanLocalState : public LocalTableFunctionState {
public:
	//! Index of the buffer assigned to this thread
	idx_t buffer_index;

public:
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state);
};

} // namespace duckdb
