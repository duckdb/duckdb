//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "json_reader_options.hpp"
#include "duckdb/common/mutex.hpp"
#include "json_common.hpp"
#include "json_enums.hpp"

namespace duckdb {
struct JSONScanGlobalState;
class JSONReader;

struct JSONBufferHandle {
public:
	JSONBufferHandle(JSONReader &reader, idx_t buffer_index, idx_t readers, AllocatedData &&buffer, idx_t buffer_size,
	                 idx_t buffer_start, idx_t file_start = 0);

public:
	//! The reader this buffer comes from
	JSONReader &reader;
	//! Buffer index (within same file)
	const idx_t buffer_index;

	//! Number of readers for this buffer
	atomic<idx_t> readers;
	//! The buffer
	AllocatedData buffer;
	//! The size of the data in the buffer (can be less than buffer.GetSize())
	const idx_t buffer_size;
	//! Buffer-local position where the fresh (just-read) data begins. Bytes
	//! before this position are reserved padding or copied remainder from the
	//! previous buffer -- not part of this read.
	idx_t buffer_start;
	//! File-level byte offset that corresponds to `buffer_ptr + buffer_start`.
	//! Used by ReadJSONFunction to translate a record pointer into a stable
	//! file byte offset for the `file_row_number` virtual column.
	idx_t file_start;
};

struct JSONFileHandle {
public:
	JSONFileHandle(QueryContext context, unique_ptr<FileHandle> file_handle, Allocator &allocator);

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

	//! The next two functions return whether the read was successful.
	//! `seek_to` makes the request random-access: the sequential cursor is
	//! left alone, and EOF does not latch `last_read_requested`.
	bool GetPositionAndSize(idx_t &position, idx_t &size, idx_t requested_size, optional_idx seek_to = optional_idx());
	bool Read(char *pointer, idx_t &read_size, idx_t requested_size);
	//! Current file position -- the next sequential Read() will begin at this offset.
	idx_t GetReadPosition() const {
		return read_position.load();
	}
	//! Read at position optionally allows passing a custom handle to read from, otherwise the default one is used
	void ReadAtPosition(char *pointer, idx_t size, idx_t position, optional_ptr<FileHandle> override_handle = nullptr);

private:
	idx_t ReadInternal(char *pointer, const idx_t requested_size);
	idx_t ReadFromCache(char *&pointer, idx_t &size, atomic<idx_t> &position);

private:
	QueryContext context;

	//! The JSON file handle
	unique_ptr<FileHandle> file_handle;
	Allocator &allocator;

	//! File properties
	const bool can_seek;
	const idx_t file_size;

	//! Read properties
	atomic<idx_t> read_position;
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

enum class JSONFileReadType { SCAN_ENTIRE_FILE, SCAN_PARTIAL };

struct JSONReaderScanState {
	explicit JSONReaderScanState(ClientContext &context, Allocator &global_allocator,
	                             idx_t reconstruct_buffer_capacity);

	FileSystem &fs;
	Allocator &global_allocator;
	//! Thread-local allocator
	JSONAllocator allocator;
	idx_t buffer_capacity;
	bool initialized = false;
	// if we have a buffer already - this is our buffer index
	optional_idx buffer_index;
	//! Whether or not we are scanning the entire file
	//! If we are scanning the entire file we don't share reads between threads and just read the file until we are done
	JSONFileReadType file_read_type = JSONFileReadType::SCAN_PARTIAL;
	// Data for reading (if we have postponed reading)
	//! Buffer (if we have one)
	AllocatedData read_buffer;
	bool needs_to_read = false;
	idx_t request_size;
	idx_t read_position;
	idx_t read_size;
	//! Current scan data
	idx_t scan_count = 0;
	JSONString units[STANDARD_VECTOR_SIZE];
	yyjson_val *values[STANDARD_VECTOR_SIZE];
	optional_ptr<JSONBufferHandle> current_buffer_handle;
	//! Current buffer read info
	optional_ptr<JSONReader> current_reader;
	char *buffer_ptr = nullptr;
	idx_t buffer_size = 0;
	idx_t buffer_offset = 0;
	idx_t prev_buffer_remainder = 0;
	idx_t prev_buffer_offset = 0;
	idx_t lines_or_objects_in_buffer = 0;
	//! Whether this is the first time scanning this buffer
	bool is_first_scan = false;
	//! Whether this is the last batch of the file
	bool is_last = false;
	//! Buffer to reconstruct split values
	optional_idx batch_index;

	//! For some filesystems (e.g. S3), using a filehandle per thread increases performance
	unique_ptr<FileHandle> thread_local_filehandle;

	//! File byte offset that the most recent physical read started at -- i.e.
	//! the file position corresponding to `buffer_ptr + buffer_offset` after
	//! FinalizeBuffer. The two Read functions set this before issuing the read;
	//! FinalizeBufferInternal propagates it into JSONBufferHandle::file_start.
	idx_t file_read_start = 0;

public:
	//! Reset for parsing the next batch of JSON from the current buffer
	void ResetForNextParse();
	//! Reset state for reading the next buffer
	void ResetForNextBuffer();
	//! Clear the buffer handle (if any)
	void ClearBufferHandle();
};

struct JSONError {
	idx_t buf_index;
	idx_t line_or_object_in_buf;
	string error_msg;
};

class JSONReader : public BaseFileReader {
public:
	JSONReader(ClientContext &context, JSONReaderOptions options, OpenFileInfo file);

	void OpenJSONFile();
	void CloseHandle();
	void Reset();

	bool HasFileHandle() const;
	bool IsOpen() const;
	bool IsInitialized() const {
		return initialized;
	}

	JSONReaderOptions &GetOptions();

	JSONFormat GetFormat() const;
	void SetFormat(JSONFormat format);

	JSONRecordType GetRecordType() const;
	void SetRecordType(JSONRecordType type);

	const string &GetFileName() const;
	JSONFileHandle &GetFileHandle() const;

public:
	string GetReaderType() const override {
		return "JSON";
	}

	void PrepareReader(ClientContext &context, GlobalTableFunctionState &) override;
	bool TryInitializeScan(ClientContext &context, GlobalTableFunctionState &gstate,
	                       LocalTableFunctionState &lstate) override;
	AsyncResult Scan(ClientContext &context, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk) override;
	void FinishFile(ClientContext &context, GlobalTableFunctionState &gstate_p) override;
	double GetProgressInFile(ClientContext &context) override;
	//! Accepts file_row_number as a virtual column and remembers the local
	//! scan_chunk slot it lands in (column_ids.size() at this point, since
	//! the framework pushes the local id immediately after this call).
	void AddVirtualColumn(column_t virtual_column_id) override;

	//! Local slot in scan_chunk for file_row_number when projected, else
	//! INVALID_INDEX. This is the post-constant-drop position -- using the
	//! global col_idx from column_indexes would overshoot in glob mode where
	//! file_index is dropped as a constant from the local layout.
	idx_t file_row_number_local_idx = DConstants::INVALID_INDEX;

public:
	//! Get a new buffer index (must hold the lock)
	idx_t GetBufferIndex();
	//! Set line count for a buffer that is done (grabs the lock)
	void SetBufferLineOrObjectCount(JSONBufferHandle &handle, idx_t count);
	//! Records a parse error in the specified buffer
	void AddParseError(JSONReaderScanState &scan_state, idx_t line_or_object_in_buf, yyjson_read_err &err,
	                   const string &extra = "");
	//! Records a transform error in the specified buffer
	void AddTransformError(JSONReaderScanState &scan_state, idx_t object_index, const string &error_message);
	//! Whether this reader has thrown if an error has occurred
	bool HasThrown();

	void Initialize(Allocator &allocator, idx_t buffer_size);
	bool InitializeScan(JSONReaderScanState &state, JSONFileReadType file_read_type);
	bool ParseJSON(JSONReaderScanState &scan_state, char *const json_start, const idx_t json_size,
	               const idx_t remaining);
	bool ParseNextChunk(JSONReaderScanState &scan_state);
	idx_t Scan(JSONReaderScanState &scan_state);
	bool ReadNextBuffer(JSONReaderScanState &scan_state);
	bool PrepareBufferForRead(JSONReaderScanState &scan_state);

	//! Scan progress
	double GetProgress() const;

	void DecrementBufferUsage(JSONBufferHandle &handle, idx_t lines_or_object_in_buffer, AllocatedData &buffer);

	//! Read ONE record at `file_offset` into scan_state.units[0] /
	//! scan_state.values[0]. Caller must consume slot 0 before the next call.
	//! Returns false on read/parse failure or boundary-crossing record.
	//! TODO: stitch boundary-crossing records with the next buffer.
	bool FetchRow(JSONReaderScanState &scan_state, idx_t file_offset);

	//! Drop FetchRow's cached buffers. Call between batches once the prior
	//! batch's output has been consumed downstream.
	void ClearLookupBuffers(JSONReaderScanState &scan_state);

private:
	void SkipOverArrayStart(JSONReaderScanState &scan_state);
	void AutoDetect(Allocator &allocator, idx_t buffer_size);
	bool CopyRemainderFromPreviousBuffer(JSONReaderScanState &scan_state);
	void FinalizeBufferInternal(JSONReaderScanState &scan_state, AllocatedData &buffer, idx_t buffer_index);
	void PrepareForReadInternal(JSONReaderScanState &scan_state);
	void PrepareForScan(JSONReaderScanState &scan_state);
	bool PrepareBufferSeek(JSONReaderScanState &scan_state);
	void ReadNextBufferSeek(JSONReaderScanState &scan_state);
	bool ReadNextBufferNoSeek(JSONReaderScanState &scan_state);
	void FinalizeBuffer(JSONReaderScanState &scan_state);
	//! Random-access variant of PrepareBufferSeek: caller-supplied offset,
	//! does not advance the sequential read cursor. False at EOF.
	bool PrepareBufferSeekAt(JSONReaderScanState &scan_state, idx_t file_offset, idx_t request_size);

	//! Insert/get/remove buffer (grabs the lock)
	void InsertBuffer(idx_t buffer_idx, unique_ptr<JSONBufferHandle> &&buffer);
	optional_ptr<JSONBufferHandle> GetBuffer(idx_t buffer_idx);
	AllocatedData RemoveBuffer(JSONBufferHandle &handle);

	void ThrowObjectSizeError(const idx_t object_size);

private:
	//! Add an error to the buffer - requires the lock to be held
	void AddError(idx_t buf_index, idx_t line_or_object_in_buf, const string &error_msg);
	//! Throw errors if possible - requires the lock to be held
	void ThrowErrorsIfPossible();
	//! Try to get the line number - requires the lock to be held
	optional_idx TryGetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf);

private:
	ClientContext &context;
	JSONReaderOptions options;

	//! File handle
	unique_ptr<JSONFileHandle> file_handle;

	//! Whether or not the reader has been initialized
	bool initialized;
	//! Next buffer index within the file
	idx_t next_buffer_index;
	//! Mapping from batch index to currently held buffers
	unordered_map<idx_t, unique_ptr<JSONBufferHandle>> buffer_map;

	//! Line count per buffer
	vector<int64_t> buffer_line_or_object_counts;
	//! Whether any of the reading threads has thrown an error
	bool thrown;

	//! If we have auto-detected, this is the buffer read by the auto-detection
	AllocatedData auto_detect_data;
	idx_t auto_detect_data_size = 0;

	//! The first error we found in the file (if any)
	unique_ptr<JSONError> error;

public:
	mutable mutex lock;
};

} // namespace duckdb
