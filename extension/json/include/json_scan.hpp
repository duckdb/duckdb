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
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/function/table_function.hpp"
#include "json_transform.hpp"

namespace duckdb {

enum class JSONScanType : uint8_t {
	INVALID = 0,
	//! Read JSON straight to columnar data
	READ_JSON = 1,
	//! Read JSON objects as strings
	READ_JSON_OBJECTS = 2,
	//! Sample run for schema detection
	SAMPLE = 3,
};

//! Even though LogicalTypeId is just a uint8_t, this is still needed ...
struct LogicalTypeIdHash {
	inline std::size_t operator()(const LogicalTypeId &id) const {
		return (size_t)id;
	}
};

struct DateFormatMap {
public:
	void Initialize(const unordered_map<LogicalTypeId, vector<const char *>, LogicalTypeIdHash> &format_templates) {
		for (const auto &entry : format_templates) {
			auto &formats = candidate_formats.emplace(entry.first, vector<StrpTimeFormat>()).first->second;
			formats.reserve(entry.second.size());
			for (const auto &format : entry.second) {
				formats.emplace_back();
				formats.back().format_specifier = format;
				StrpTimeFormat::ParseFormatSpecifier(formats.back().format_specifier, formats.back());
			}
		}
	}

	bool HasFormats(LogicalTypeId type) const {
		return candidate_formats.find(type) != candidate_formats.end();
	}

	vector<StrpTimeFormat> &GetCandidateFormats(LogicalTypeId type) {
		D_ASSERT(HasFormats(type));
		return candidate_formats[type];
	}

	StrpTimeFormat &GetFormat(LogicalTypeId type) {
		return candidate_formats[type].back();
	}

private:
	unordered_map<LogicalTypeId, vector<StrpTimeFormat>, LogicalTypeIdHash> candidate_formats;
};

struct JSONScanData : public TableFunctionData {
public:
	JSONScanData();

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input);
	static void InitializeFilePaths(ClientContext &context, const vector<string> &patterns, vector<string> &file_paths);

	void Serialize(FieldWriter &writer);
	void Deserialize(FieldReader &reader);

public:
	//! Scan type
	JSONScanType type;
	//! File-specific options
	BufferedJSONReaderOptions options;
	//! The files we're reading
	vector<string> file_paths;

	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 1MB minimum)
	idx_t maximum_object_size = 1048576;
	//! Options when transforming the JSON to columnar data
	JSONTransformOptions transform_options;

	//! Whether we auto-detect a schema
	bool auto_detect = false;
	//! Sample size for detecting schema
	idx_t sample_size = STANDARD_VECTOR_SIZE;
	//! Column names (in order)
	vector<string> names;
	//! Max depth we go to detect nested JSON schema (defaults to unlimited)
	idx_t max_depth = NumericLimits<idx_t>::Maximum();
	//! Whether we're parsing objects (usually), or something else like arrays
	bool objects = true;

	//! Stored readers for when we're detecting the schema
	vector<unique_ptr<BufferedJSONReader>> stored_readers;
	//! Candidate date formats
	DateFormatMap date_format_map;
};

struct JSONScanInfo : public TableFunctionInfo {
public:
	explicit JSONScanInfo(JSONScanType type_p = JSONScanType::INVALID, JSONFormat format_p = JSONFormat::AUTO_DETECT,
	                      bool auto_detect_p = false)
	    : type(type_p), format(format_p), auto_detect(auto_detect_p) {
	}

	JSONScanType type;
	JSONFormat format;
	bool auto_detect;
};

struct JSONScanGlobalState {
public:
	JSONScanGlobalState(ClientContext &context, JSONScanData &bind_data);

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
	atomic<idx_t> batch_index;

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

struct JSONScanLocalState {
public:
	JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate);

public:
	idx_t ReadNext(JSONScanGlobalState &gstate);
	yyjson_alc *GetAllocator();
	void ThrowTransformError(idx_t count, idx_t object_index, const string &error_message);

	JSONLine lines[STANDARD_VECTOR_SIZE];
	yyjson_val *objects[STANDARD_VECTOR_SIZE];

	idx_t batch_index;

	//! Options when transforming the JSON to columnar data
	DateFormatMap date_format_map;
	JSONTransformOptions transform_options;

private:
	yyjson_val *ParseLine(char *line_start, idx_t line_size, idx_t remaining, JSONLine &line);

private:
	//! Bind data
	JSONScanData &bind_data;
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
	idx_t lines_or_objects_in_buffer;

	//! Buffer to reconstruct split objects
	AllocatedData reconstruct_buffer;
	//! Copy of current buffer for YYJSON_READ_INSITU
	AllocatedData current_buffer_copy;
	const char *buffer_copy_ptr;

private:
	bool ReadNextBuffer(JSONScanGlobalState &gstate);
	void ReadNextBuffer(JSONScanGlobalState &gstate, idx_t &buffer_index);
	void ReadNextBufferSeek(JSONScanGlobalState &gstate, idx_t &buffer_index);
	void ReadNextBufferNoSeek(JSONScanGlobalState &gstate, idx_t &buffer_index);

	void ReconstructFirstObject(JSONScanGlobalState &gstate);

	void ReadUnstructured(idx_t &count);
	void ReadNewlineDelimited(idx_t &count);
};

struct JSONGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	JSONGlobalTableFunctionState(ClientContext &context, TableFunctionInitInput &input);
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
	idx_t MaxThreads() const override;

public:
	JSONScanGlobalState state;
};

struct JSONLocalTableFunctionState : public LocalTableFunctionState {
public:
	JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate);
	static unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                GlobalTableFunctionState *global_state);
	idx_t GetBatchIndex() const;

public:
	JSONScanLocalState state;
};

struct JSONScan {
public:
	static double JSONScanProgress(ClientContext &context, const FunctionData *bind_data_p,
	                               const GlobalTableFunctionState *global_state) {
		auto &gstate = ((JSONGlobalTableFunctionState &)*global_state).state;
		double progress = 0;
		for (auto &reader : gstate.json_readers) {
			progress += reader->GetProgress();
		}
		return progress / double(gstate.json_readers.size());
	}

	static idx_t JSONScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                   LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
		auto &lstate = (JSONLocalTableFunctionState &)*local_state;
		return lstate.GetBatchIndex();
	}

	static void JSONScanSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
		auto &bind_data = (JSONScanData &)*bind_data_p;
		bind_data.Serialize(writer);
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
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;

		table_function.table_scan_progress = JSONScanProgress;
		table_function.get_batch_index = JSONScanGetBatchIndex;

		table_function.serialize = JSONScanSerialize;
		table_function.deserialize = JSONScanDeserialize;

		// TODO: might be able to do some of these
		table_function.projection_pushdown = false;
		table_function.filter_pushdown = false;
		table_function.filter_prune = false;
	}
};

} // namespace duckdb
