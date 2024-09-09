//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "buffered_json_reader.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/type_map.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/function/table_function.hpp"
#include "json_enums.hpp"
#include "json_transform.hpp"

namespace duckdb {

struct JSONString {
public:
	JSONString() {
	}
	JSONString(const char *pointer_p, idx_t size_p) : pointer(pointer_p), size(size_p) {
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

struct DateFormatMap {
public:
	void Initialize(const type_id_map_t<vector<const char *>> &format_templates) {
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
		D_ASSERT(candidate_formats.find(type) != candidate_formats.end());
		return candidate_formats.find(type)->second.back();
	}

	const StrpTimeFormat &GetFormat(LogicalTypeId type) const {
		D_ASSERT(candidate_formats.find(type) != candidate_formats.end());
		return candidate_formats.find(type)->second.back();
	}

private:
	type_id_map_t<vector<StrpTimeFormat>> candidate_formats;
};

struct JSONScanData : public TableFunctionData {
public:
	JSONScanData();

	void Bind(ClientContext &context, TableFunctionBindInput &input);

	void InitializeReaders(ClientContext &context);
	void InitializeFormats();
	void InitializeFormats(bool auto_detect);
	void SetCompression(const string &compression);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<JSONScanData> Deserialize(Deserializer &deserializer);

public:
	//! Scan type
	JSONScanType type;

	//! File-specific options
	BufferedJSONReaderOptions options;

	//! Multi-file reader stuff
	MultiFileReaderBindData reader_bind;

	//! The files we're reading
	vector<string> files;
	//! Initial file reader
	unique_ptr<BufferedJSONReader> initial_reader;
	//! The readers
	vector<unique_ptr<BufferedJSONReader>> union_readers;

	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 16MB minimum)
	idx_t maximum_object_size = 16777216;
	//! Whether we auto-detect a schema
	bool auto_detect = false;
	//! Sample size for detecting schema
	idx_t sample_size = idx_t(STANDARD_VECTOR_SIZE) * 10;
	//! Max depth we go to detect nested JSON schema (defaults to unlimited)
	idx_t max_depth = NumericLimits<idx_t>::Maximum();
	//! We divide the number of appearances of each JSON field by the auto-detection sample size
	//! If the average over the fields of an object is less than this threshold,
	//! we default to the MAP type with value type of merged field types
	double field_appearance_threshold = 0.1;
	//! The maximum number of files we sample to sample sample_size rows
	idx_t maximum_sample_files = 32;
	//! Whether we auto-detect and convert JSON strings to integers
	bool convert_strings_to_integers = false;
	//! If a struct contains more fields than this threshold with at least 80% similar types,
	//! we infer it as MAP type
	idx_t map_inference_threshold = 25;

	//! All column names (in order)
	vector<string> names;
	//! Options when transforming the JSON to columnar data
	JSONTransformOptions transform_options;
	//! Forced date/timestamp formats
	string date_format;
	string timestamp_format;
	//! Candidate date formats
	DateFormatMap date_format_map;

	//! The inferred avg tuple size
	idx_t avg_tuple_size = 420;

private:
	JSONScanData(ClientContext &context, vector<string> files, string date_format, string timestamp_format);

	string GetDateFormat() const;
	string GetTimestampFormat() const;
};

struct JSONScanInfo : public TableFunctionInfo {
public:
	explicit JSONScanInfo(JSONScanType type_p = JSONScanType::INVALID, JSONFormat format_p = JSONFormat::AUTO_DETECT,
	                      JSONRecordType record_type_p = JSONRecordType::AUTO_DETECT, bool auto_detect_p = false)
	    : type(type_p), format(format_p), record_type(record_type_p), auto_detect(auto_detect_p) {
	}

	JSONScanType type;
	JSONFormat format;
	JSONRecordType record_type;
	bool auto_detect;
};

struct JSONScanGlobalState {
public:
	JSONScanGlobalState(ClientContext &context, const JSONScanData &bind_data);

public:
	//! Bound data
	const JSONScanData &bind_data;
	//! Options when transforming the JSON to columnar data
	JSONTransformOptions transform_options;

	//! Column names that we're actually reading (after projection pushdown)
	vector<string> names;
	vector<column_t> column_indices;

	//! Buffer manager allocator
	Allocator &allocator;
	//! The current buffer capacity
	idx_t buffer_capacity;

	mutex lock;
	//! One JSON reader per file
	vector<optional_ptr<BufferedJSONReader>> json_readers;
	//! Current file/batch index
	atomic<idx_t> file_index;
	atomic<idx_t> batch_index;

	//! Current number of threads active
	idx_t system_threads;
	//! Whether we enable parallel scans (only if less files than threads)
	bool enable_parallel_scans;
};

struct JSONScanLocalState {
public:
	JSONScanLocalState(ClientContext &context, JSONScanGlobalState &gstate);

public:
	idx_t ReadNext(JSONScanGlobalState &gstate);
	void ThrowTransformError(idx_t object_index, const string &error_message);

	yyjson_alc *GetAllocator();
	const MultiFileReaderData &GetReaderData() const;

public:
	//! Current scan data
	idx_t scan_count;
	JSONString units[STANDARD_VECTOR_SIZE];
	yyjson_val *values[STANDARD_VECTOR_SIZE];

	//! Batch index for order-preserving parallelism
	idx_t batch_index;

	//! Options when transforming the JSON to columnar data
	DateFormatMap date_format_map;
	JSONTransformOptions transform_options;

	//! For determining average tuple size
	idx_t total_read_size;
	idx_t total_tuple_count;

private:
	bool ReadNextBuffer(JSONScanGlobalState &gstate);
	bool ReadNextBufferInternal(JSONScanGlobalState &gstate, AllocatedData &buffer, optional_idx &buffer_index,
	                            bool &file_done);
	bool ReadNextBufferSeek(JSONScanGlobalState &gstate, AllocatedData &buffer, optional_idx &buffer_index,
	                        bool &file_done);
	bool ReadNextBufferNoSeek(JSONScanGlobalState &gstate, AllocatedData &buffer, optional_idx &buffer_index,
	                          bool &file_done);
	AllocatedData AllocateBuffer(JSONScanGlobalState &gstate);
	data_ptr_t GetReconstructBuffer(JSONScanGlobalState &gstate);

	void SkipOverArrayStart();

	void ReadAndAutoDetect(JSONScanGlobalState &gstate, AllocatedData &buffer, optional_idx &buffer_index,
	                       bool &file_done);
	bool ReconstructFirstObject(JSONScanGlobalState &gstate);
	void ParseNextChunk(JSONScanGlobalState &gstate);

	void ParseJSON(char *const json_start, const idx_t json_size, const idx_t remaining);
	void ThrowObjectSizeError(const idx_t object_size);

	//! Must hold the lock
	void TryIncrementFileIndex(JSONScanGlobalState &gstate) const;
	bool IsParallel(JSONScanGlobalState &gstate) const;

private:
	//! Bind data
	const JSONScanData &bind_data;
	//! Thread-local allocator
	JSONAllocator allocator;

	//! Current reader and buffer handle
	optional_ptr<BufferedJSONReader> current_reader;
	optional_ptr<JSONBufferHandle> current_buffer_handle;
	//! Whether this is the last batch of the file
	bool is_last;

	//! The current main filesystem
	FileSystem &fs;

	//! For some filesystems (e.g. S3), using a filehandle per thread increases performance
	unique_ptr<FileHandle> thread_local_filehandle;

	//! Current buffer read info
	char *buffer_ptr;
	idx_t buffer_size;
	idx_t buffer_offset;
	idx_t prev_buffer_remainder;
	idx_t lines_or_objects_in_buffer;

	//! Buffer to reconstruct split values
	AllocatedData reconstruct_buffer;
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
	static void AutoDetect(ClientContext &context, JSONScanData &bind_data, vector<LogicalType> &return_types,
	                       vector<string> &names);

	static double ScanProgress(ClientContext &context, const FunctionData *bind_data_p,
	                           const GlobalTableFunctionState *global_state);
	static idx_t GetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                           LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state);
	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *bind_data);
	static void ComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                  vector<unique_ptr<Expression>> &filters);

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                      const TableFunction &function);
	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &function);

	static void TableFunctionDefaults(TableFunction &table_function);
};

} // namespace duckdb
