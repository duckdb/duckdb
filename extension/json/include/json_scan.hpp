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
	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input);
	static void InitializeFilePaths(ClientContext &context, const vector<string> &patterns, vector<string> &file_paths);

public:
	//! The JSON reader options
	BufferedJSONReaderOptions options;
};

struct JSONScanInfo : public TableFunctionInfo {
public:
	explicit JSONScanInfo(JSONFormat forced_format_p, bool return_strings)
	    : forced_format(forced_format_p), return_json_strings(return_strings) {
	}

	JSONFormat forced_format;
	bool return_json_strings;
};

struct JSONBufferHandle {
public:
	JSONBufferHandle(idx_t file_index, idx_t readers, AllocatedData &&buffer);

public:
	idx_t file_index;
	atomic<idx_t> readers;
	AllocatedData buffer;
};

struct JSONScanGlobalState : public GlobalTableFunctionState {
public:
	JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data);
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);

	idx_t MaxThreads() const override;

public:
	//! The current buffer capacity
	idx_t buffer_capacity;

	mutex lock;
	//! The JSON reader
	unique_ptr<BufferedJSONReader> json_reader;
	//! Next batch index
	idx_t batch_index;
	//! Mapping from batch index to currently held buffers
	unordered_map<idx_t, unique_ptr<JSONBufferHandle>> buffer_map;
	//! Buffer manager allocator
	Allocator &allocator;
};

struct JSONLine {
public:
	const char *pointer;
	idx_t size;

public:
	string ToString() {
		return string(pointer, size);
	}

	const char &operator[](size_t i) const {
		return pointer[i];
	}
};

struct JSONScanLocalState : public LocalTableFunctionState {
public:
	explicit JSONScanLocalState(JSONScanGlobalState &gstate);
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state);
	idx_t ReadNext(JSONScanGlobalState &gstate);
	idx_t GetBatchIndex() const;

	JSONLine lines[STANDARD_VECTOR_SIZE];
	vector<DocPointer<yyjson_doc>> objects;

private:
	//! Batch index assigned to this thread and associate buffer handle
	idx_t batch_index;
	JSONBufferHandle *current_buffer_handle;
	//! Whether this is the last batch of the file
	bool is_last;

	//! Current batch read stuff
	const char *buffer_ptr;
	idx_t buffer_size;
	idx_t buffer_offset;
	idx_t prev_buffer_remainder;

	//! Buffer to reconstruct first object
	AllocatedData reconstruct_buffer;

private:
	bool ReadNextBuffer(JSONScanGlobalState &gstate, bool &first_read);
	void ReadNextBufferSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &file_index, idx_t &next_batch_index,
	                        idx_t &readers);
	void ReadNextBufferNoSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &file_index, idx_t &next_batch_index,
	                          idx_t &readers);

	void ReconstructFirstObject(JSONScanGlobalState &gstate);

	void ReadUnstructured(idx_t &count, const BufferedJSONReaderOptions &options);
	void ReadNewlineDelimited(idx_t &count, const BufferedJSONReaderOptions &options);
};

struct JSONScan {
public:
	static double JSONScanProgress(ClientContext &context, const FunctionData *bind_data_p,
	                               const GlobalTableFunctionState *global_state) {
		auto &gstate = (JSONScanGlobalState &)*global_state;
		return gstate.json_reader->GetProgress();
	}

	static idx_t JSONScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                   LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
		auto &lstate = (JSONScanLocalState &)*local_state;
		return lstate.GetBatchIndex();
	}

	static void JSONScanSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
		auto &bind_data = (JSONScanData &)*bind_data_p;
		bind_data.options.Serialize(writer);
	}

	static unique_ptr<FunctionData> JSONScanDeserialize(ClientContext &context, FieldReader &reader,
	                                                    TableFunction &function) {
		BufferedJSONReaderOptions options;
		options.Deserialize(reader);
		return make_unique<JSONScanData>(options);
	}

	static void TableFunctionDefaults(TableFunction &table_function) {
		table_function.named_parameters["maximum_object_size"] = LogicalType::UINTEGER;

		table_function.table_scan_progress = JSONScanProgress;
		table_function.get_batch_index = JSONScanGetBatchIndex;

		table_function.serialize = JSONScanSerialize;
		table_function.deserialize = JSONScanDeserialize;

		table_function.projection_pushdown = false;
		table_function.filter_pushdown = false;
		table_function.filter_prune = false;
	}
};

} // namespace duckdb
