//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "buffered_json_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct JSONScanLocalState;

struct JSONScanData : public TableFunctionData {
public:
	JSONScanData();

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input);
	static void InitializeFilePaths(ClientContext &context, const vector<string> &patterns, vector<string> &file_paths);

	void Serialize(FieldWriter &writer);
	void Deserialize(FieldReader &reader);

public:
	//! File-specific options
	BufferedJSONReaderOptions options;
	//! The files we're reading
	vector<string> file_paths;

	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 1MB)
	idx_t maximum_object_size = 1048576;
	//! Whether we return JSON strings (if not, we return YYJSON documents)
	bool return_json_strings = true;
};

struct JSONScanInfo : public TableFunctionInfo {
public:
	explicit JSONScanInfo(JSONFormat forced_format_p, bool return_strings)
	    : forced_format(forced_format_p), return_json_strings(return_strings) {
	}

	JSONFormat forced_format;
	bool return_json_strings;
};

struct JSONScanGlobalState : public GlobalTableFunctionState {
public:
	JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data);
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
	idx_t MaxThreads() const override;

public:
	//! Bound data
	JSONScanData &bind_data;

	//! Buffer manager allocator
	Allocator &allocator;
	//! The current buffer capacity
	idx_t buffer_capacity;

	mutex lock;
	//! One JSON reader per file
	vector<unique_ptr<BufferedJSONReader>> json_readers;
	//! Current file/batch index
	idx_t file_index;
	idx_t batch_index;

	//! Current number of threads active
	idx_t system_threads;
};

struct JSONLine {
public:
	JSONLine() {
	}
	JSONLine(const char *pointer_p, idx_t size_p) : pointer(pointer_p), size(size_p) {
	}

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
	JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate);
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state);
	idx_t ReadNext(JSONScanGlobalState &gstate);
	idx_t GetBatchIndex() const;

	JSONLine lines[STANDARD_VECTOR_SIZE];
	yyjson_doc *objects[STANDARD_VECTOR_SIZE];

	idx_t batch_index;

private:
	yyjson_doc *ParseLine(char *line_start, idx_t line_size, JSONLine &line, const bool &ignore_errors);

private:
	//! Thread-local allocator
	JSONAllocator json_allocator;

	//! Current reader and buffer handle
	BufferedJSONReader *current_reader;
	JSONBufferHandle *current_buffer_handle;
	//! Whether this is the last batch of the file
	bool is_last;

	//! Current buffer read info
	const char *buffer_ptr;
	idx_t buffer_size;
	idx_t buffer_offset;
	idx_t prev_buffer_remainder;

	//! Buffer to reconstruct split objects
	AllocatedData reconstruct_buffer;
	//! Copy of current buffer for YYJSON_READ_INSITU
	AllocatedData current_buffer_copy;
	const char *buffer_copy_ptr;

private:
	bool ReadNextBuffer(JSONScanGlobalState &gstate, bool &first_read);
	void ReadNextBufferSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &buffer_index);
	void ReadNextBufferNoSeek(JSONScanGlobalState &gstate, bool &first_read, idx_t &buffer_index);

	void ReconstructFirstObject(JSONScanGlobalState &gstate);

	void ReadUnstructured(idx_t &count);
	void ReadNewlineDelimited(idx_t &count, const bool &ignore_errors);
};

struct JSONScan {
public:
	static double JSONScanProgress(ClientContext &context, const FunctionData *bind_data_p,
	                               const GlobalTableFunctionState *global_state) {
		auto &gstate = (JSONScanGlobalState &)*global_state;
		double progress = 0;
		for (auto &reader : gstate.json_readers) {
			progress += reader->GetProgress();
		}
		return progress / double(gstate.json_readers.size());
	}

	static idx_t JSONScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                   LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
		auto &lstate = (JSONScanLocalState &)*local_state;
		return lstate.GetBatchIndex();
	}

	static void JSONScanSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
		auto &bind_data = (JSONScanData &)*bind_data_p;
		bind_data.Serialize(writer);
		bind_data.options.Serialize(writer);
	}

	static unique_ptr<FunctionData> JSONScanDeserialize(ClientContext &context, FieldReader &reader,
	                                                    TableFunction &function) {
		auto result = make_unique<JSONScanData>();
		result->Deserialize(reader);
		return std::move(result);
	}

	static void TableFunctionDefaults(TableFunction &table_function) {
		table_function.named_parameters["maximum_object_size"] = LogicalType::UINTEGER;
		table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
		table_function.named_parameters["format"] = LogicalType::VARCHAR;

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
