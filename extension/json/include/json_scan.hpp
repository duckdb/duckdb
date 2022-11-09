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

namespace duckdb {

struct JSONScanLocalState;

struct JSONScanData : public TableFunctionData {
public:
	explicit JSONScanData(BufferedJSONReaderOptions options);
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names);

public:
	//! The JSON reader options
	BufferedJSONReaderOptions options;
};

struct JSONBufferHandle {
public:
	JSONBufferHandle(idx_t readers, AllocatedData &&buffer);

public:
	atomic<idx_t> readers;
	AllocatedData buffer;
};

struct JSONScanGlobalState : public GlobalTableFunctionState {
public:
	JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data);
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

public:
	//! Initial buffer capacity (1MB)
	static constexpr idx_t INITIAL_BUFFER_CAPACITY = 1048576;
	//! The current buffer capacity
	idx_t buffer_capacity;

	mutex lock;
	//! The JSON reader
	unique_ptr<BufferedJSONReader> json_reader;
	//! Next batch index
	idx_t batch_index;
	//! Mapping from batch index to currently held buffers
	unordered_map<idx_t, JSONBufferHandle> buffer_map;
	//! Buffer manager allocator
	Allocator &allocator;
};

struct JSONLine {
	data_ptr_t pointer;
	idx_t size;
};

struct JSONScanLocalState : public LocalTableFunctionState {
public:
	JSONScanLocalState();
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state);
	idx_t ReadNext(JSONScanGlobalState &gstate);

private:
	//! Batch index assigned to this thread
	idx_t batch_index;
	//! Buffer handle associated with this batch index
	JSONBufferHandle *current_buffer_handle;
	//! Buffer handle associate with the previous batch index
	JSONBufferHandle *previous_buffer_handle;

	//! Current batch read stuff
	idx_t read_position;
	idx_t read_size;
	JSONLine lines[STANDARD_VECTOR_SIZE];
};

} // namespace duckdb
