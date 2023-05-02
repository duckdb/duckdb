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
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/table_function.hpp"
#include "json_transform.hpp"

namespace duckdb {

enum class JSONScanType : uint8_t {
	INVALID = 0,
	//! Read JSON straight to columnar data
	READ_JSON = 1,
	//! Read JSON values as strings
	READ_JSON_OBJECTS = 2,
	//! Sample run for schema detection
	SAMPLE = 3,
};

enum class JSONRecordType : uint8_t {
	//! Sequential values
	RECORDS = 0,
	//! Array of values
	ARRAY_OF_RECORDS = 1,
	//! Sequential non-object JSON
	JSON = 2,
	//! Array of non-object JSON
	ARRAY_OF_JSON = 3,
	//! Auto-detect
	AUTO = 4,
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
			const auto &type = entry.first;
			for (const auto &format_string : entry.second) {
				AddFormat(type, format_string);
			}
		}
	}

	void AddFormat(LogicalTypeId type, const string &format_string) {
		auto &formats = candidate_formats[type];
		formats.emplace_back();
		formats.back().format_specifier = format_string;
		StrpTimeFormat::ParseFormatSpecifier(formats.back().format_specifier, formats.back());
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
	void InitializeFormats();

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
	//! Valid cols (ROW_TYPE cols are considered invalid)
	vector<idx_t> valid_cols;
	//! Max depth we go to detect nested JSON schema (defaults to unlimited)
	idx_t max_depth = NumericLimits<idx_t>::Maximum();
	//! Whether we're parsing values (usually), or something else
	JSONRecordType record_type = JSONRecordType::RECORDS;
	//! Forced date/timestamp formats
	string date_format;
	string timestamp_format;

	//! Stored readers for when we're detecting the schema
	vector<duckdb::unique_ptr<BufferedJSONReader>> stored_readers;
	//! Candidate date formats
	DateFormatMap date_format_map;
};

struct JSONScanInfo : public TableFunctionInfo {
public:
	explicit JSONScanInfo(JSONScanType type_p = JSONScanType::INVALID, JSONFormat format_p = JSONFormat::AUTO_DETECT,
	                      JSONRecordType record_type_p = JSONRecordType::AUTO, bool auto_detect_p = false)
	    : type(type_p), format(format_p), record_type(record_type_p), auto_detect(auto_detect_p) {
	}

	JSONScanType type;
	JSONFormat format;
	JSONRecordType record_type;
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
	vector<duckdb::unique_ptr<BufferedJSONReader>> json_readers;
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
	void ThrowTransformError(idx_t object_index, const string &error_message);

	idx_t scan_count;
	JSONLine lines[STANDARD_VECTOR_SIZE];
	yyjson_val *values[STANDARD_VECTOR_SIZE];

	idx_t array_idx;
	idx_t array_offset;
	yyjson_val *array_values[STANDARD_VECTOR_SIZE];

	idx_t batch_index;

	//! Options when transforming the JSON to columnar data
	DateFormatMap date_format_map;
	JSONTransformOptions transform_options;

private:
	yyjson_val *ParseLine(char *line_start, idx_t line_size, idx_t remaining, JSONLine &line);
	idx_t GetObjectsFromArray(JSONScanGlobalState &gstate);

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

	//! Buffer to reconstruct split values
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
	static duckdb::unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input);
	idx_t MaxThreads() const override;

public:
	JSONScanGlobalState state;
};

struct JSONLocalTableFunctionState : public LocalTableFunctionState {
public:
	JSONLocalTableFunctionState(ClientContext &context, JSONScanGlobalState &gstate);
	static duckdb::unique_ptr<LocalTableFunctionState> Init(ExecutionContext &context, TableFunctionInitInput &input,
	                                                        GlobalTableFunctionState *global_state);
	idx_t GetBatchIndex() const;

public:
	JSONScanLocalState state;
};

struct JSONScan {
public:
	static void AutoDetect(ClientContext &context, JSONScanData &bind_data, vector<LogicalType> &return_types,
	                       vector<string> &names);

	static void InitializeBindData(ClientContext &context, JSONScanData &bind_data,
	                               const named_parameter_map_t &named_parameters, vector<string> &names,
	                               vector<LogicalType> &return_types);

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

	static unique_ptr<NodeStatistics> JSONScanCardinality(ClientContext &context, const FunctionData *bind_data) {
		auto &data = (JSONScanData &)*bind_data;
		idx_t per_file_cardinality;
		if (data.stored_readers.empty()) {
			// The cardinality of an unknown JSON file is the almighty number 42 except when it's not
			per_file_cardinality = 42;
		} else {
			// If we multiply the almighty number 42 by 10, we get the exact average size of a JSON
			// Not really, but the average size of a lineitem row in JSON is around 360 bytes
			per_file_cardinality = data.stored_readers[0]->GetFileHandle().FileSize() / 420;
		}
		// Obviously this can be improved but this is better than defaulting to 0
		return make_uniq<NodeStatistics>(per_file_cardinality * data.file_paths.size());
	}

	static void JSONScanSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
		auto &bind_data = (JSONScanData &)*bind_data_p;
		bind_data.Serialize(writer);
	}

	static duckdb::unique_ptr<FunctionData> JSONScanDeserialize(ClientContext &context, FieldReader &reader,
	                                                            TableFunction &function) {
		auto result = make_uniq<JSONScanData>();
		result->Deserialize(reader);
		return std::move(result);
	}

	static void TableFunctionDefaults(TableFunction &table_function) {
		table_function.named_parameters["maximum_object_size"] = LogicalType::UINTEGER;
		table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
		table_function.named_parameters["lines"] = LogicalType::VARCHAR;
		table_function.named_parameters["compression"] = LogicalType::VARCHAR;

		table_function.table_scan_progress = JSONScanProgress;
		table_function.get_batch_index = JSONScanGetBatchIndex;
		table_function.cardinality = JSONScanCardinality;

		table_function.serialize = JSONScanSerialize;
		table_function.deserialize = JSONScanDeserialize;

		table_function.projection_pushdown = false;
		table_function.filter_pushdown = false;
		table_function.filter_prune = false;
	}
};

} // namespace duckdb
