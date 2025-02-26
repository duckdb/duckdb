//===----------------------------------------------------------------------===//
//                         DuckDB
//
// buffered_json_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/base_file_reader.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "json_reader_options.hpp"
#include "duckdb/common/mutex.hpp"
#include "json_common.hpp"
#include "json_enums.hpp"

namespace duckdb {
class JSONScanGlobalState;

struct JSONBufferHandle {
public:
	JSONBufferHandle(idx_t buffer_index, idx_t readers, AllocatedData &&buffer, idx_t buffer_size);

public:
	//! Buffer index (within same file)
	const idx_t buffer_index;

	//! Number of readers for this buffer
	atomic<idx_t> readers;
	//! The buffer
	AllocatedData buffer;
	//! The size of the data in the buffer (can be less than buffer.GetSize())
	const idx_t buffer_size;
};

struct JSONFileHandle {
public:
	JSONFileHandle(unique_ptr<FileHandle> file_handle, Allocator &allocator);

	bool IsOpen() const;
	void Close();

	void Reset();
	bool RequestedReadsComplete();
	bool LastReadRequested() const;

	idx_t FileSize() const;
	idx_t Remaining() const;

	bool CanSeek() const;
	bool IsPipe() const;

	FileHandle &GetHandle();

	//! The next two functions return whether the read was successful
	bool GetPositionAndSize(idx_t &position, idx_t &size, idx_t requested_size);
	bool Read(char *pointer, idx_t &read_size, idx_t requested_size, bool &file_done, bool sample_run);
	//! Read at position optionally allows passing a custom handle to read from, otherwise the default one is used
	void ReadAtPosition(char *pointer, idx_t size, idx_t position, bool &file_done, bool sample_run,
	                    optional_ptr<FileHandle> override_handle = nullptr);

private:
	idx_t ReadInternal(char *pointer, const idx_t requested_size);
	idx_t ReadFromCache(char *&pointer, idx_t &size, idx_t &position);

private:
	//! The JSON file handle
	unique_ptr<FileHandle> file_handle;
	Allocator &allocator;

	//! File properties
	const bool can_seek;
	const idx_t file_size;

	//! Read properties
	idx_t read_position;
	atomic<idx_t> requested_reads;
	atomic<idx_t> actual_reads;
	atomic<bool> last_read_requested;

	//! Cached buffers for resetting when reading stream
	vector<AllocatedData> cached_buffers;
	idx_t cached_size;
};

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

struct JSONReaderScanState {
	explicit JSONReaderScanState(ClientContext &context, Allocator &global_allocator,
	                             idx_t reconstruct_buffer_capacity);

	FileSystem &fs;
	Allocator &global_allocator;
	//! Thread-local allocator
	JSONAllocator allocator;
	idx_t buffer_capacity;
	//! Current scan data
	idx_t scan_count = 0;
	JSONString units[STANDARD_VECTOR_SIZE];
	yyjson_val *values[STANDARD_VECTOR_SIZE];
	optional_ptr<JSONBufferHandle> current_buffer_handle;
	//! Current buffer read info
	char *buffer_ptr = nullptr;
	idx_t buffer_size = 0;
	idx_t buffer_offset = 0;
	idx_t prev_buffer_remainder = 0;
	idx_t lines_or_objects_in_buffer = 0;
	//! Whether this is the last batch of the file
	bool is_last = false;
	//! Buffer to reconstruct split values
	AllocatedData reconstruct_buffer;
	optional_idx batch_index;

	//! For some filesystems (e.g. S3), using a filehandle per thread increases performance
	unique_ptr<FileHandle> thread_local_filehandle;

public:
	data_ptr_t GetReconstructBuffer();
	void Reset();
};

class BufferedJSONReader : public BaseFileReader {
public:
	BufferedJSONReader(ClientContext &context, JSONReaderOptions options, string file_name);

	void OpenJSONFile();
	void Reset();

	bool HasFileHandle() const;
	bool IsOpen() const;

	JSONReaderOptions &GetOptions();

	JSONFormat GetFormat() const;
	void SetFormat(JSONFormat format);

	JSONRecordType GetRecordType() const;
	void SetRecordType(JSONRecordType type);

	const string &GetFileName() const;
	JSONFileHandle &GetFileHandle() const;

public:
	//! Insert/get/remove buffer (grabs the lock)
	void InsertBuffer(idx_t buffer_idx, unique_ptr<JSONBufferHandle> &&buffer);
	optional_ptr<JSONBufferHandle> GetBuffer(idx_t buffer_idx);
	AllocatedData RemoveBuffer(JSONBufferHandle &handle);

	//! Get a new buffer index (must hold the lock)
	idx_t GetBufferIndex();
	//! Set line count for a buffer that is done (grabs the lock)
	void SetBufferLineOrObjectCount(JSONBufferHandle &handle, idx_t count);
	//! Throws a parse error that mentions the file name and line number
	void ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf, yyjson_read_err &err, const string &extra = "");
	//! Throws a transform error that mentions the file name and line number
	void ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf, const string &error_message);
	//! Whether this reader has thrown if an error has occurred
	bool HasThrown();

	void ParseJSON(JSONReaderScanState &scan_state, char *const json_start, const idx_t json_size,
	               const idx_t remaining);
	void SkipOverArrayStart(JSONReaderScanState &scan_state);
	void ThrowTransformError(JSONReaderScanState &scan_state, idx_t object_index, const string &error_message);
	void AutoDetect(JSONReaderScanState &scan_state, optional_idx buffer_index);
	bool ReconstructFirstObject(JSONReaderScanState &scan_state);
	void ParseNextChunk(JSONReaderScanState &scan_state);
	void ThrowObjectSizeError(const idx_t object_size);
	void InitializeScan(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer,
	                    optional_idx &buffer_index, bool &file_done);
	bool ReadNextBuffer(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer,
	                    optional_idx &buffer_index, bool &file_done);
	bool ReadNextBufferInternal(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer,
	                            optional_idx &buffer_index, bool &file_done);
	void FinalizeBufferInternal(JSONReaderScanState &scan_state, AllocatedData &buffer, idx_t buffer_index);

	//! Scan progress
	double GetProgress() const;

private:
	bool ReadNextBufferSeek(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer,
	                        optional_idx &buffer_index, bool &file_done);
	bool ReadNextBufferNoSeek(JSONScanGlobalState &gstate, JSONReaderScanState &scan_state, AllocatedData &buffer,
	                          optional_idx &buffer_index, bool &file_done);

private:
	idx_t GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf);

private:
	ClientContext &context;
	JSONReaderOptions options;

	//! File handle
	unique_ptr<JSONFileHandle> file_handle;

	//! Next buffer index within the file
	idx_t buffer_index;
	//! Mapping from batch index to currently held buffers
	unordered_map<idx_t, unique_ptr<JSONBufferHandle>> buffer_map;

	//! Line count per buffer
	vector<int64_t> buffer_line_or_object_counts;
	//! Whether any of the reading threads has thrown an error
	bool thrown;

public:
	mutable mutex lock;
};

} // namespace duckdb
