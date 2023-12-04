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
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"
#include "json_common.hpp"
#include "json_enums.hpp"

namespace duckdb {

struct BufferedJSONReaderOptions {
public:
	//! The format of the JSON
	JSONFormat format = JSONFormat::AUTO_DETECT;
	//! Whether record types in the JSON
	JSONRecordType record_type = JSONRecordType::AUTO_DETECT;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Multi-file reader options
	MultiFileReaderOptions file_options;

public:
	void Serialize(Serializer &serializer) const;
	static BufferedJSONReaderOptions Deserialize(Deserializer &deserializer);
};

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

	idx_t FileSize() const;
	idx_t Remaining() const;

	bool CanSeek() const;

	idx_t GetPositionAndSize(idx_t &position, idx_t requested_size);
	void ReadAtPosition(char *pointer, idx_t size, idx_t position, bool sample_run);
	idx_t Read(char *pointer, idx_t requested_size, bool sample_run);

private:
	idx_t ReadInternal(char *pointer, const idx_t requested_size);
	idx_t ReadFromCache(char *&pointer, idx_t &size, idx_t &position);

private:
	//! The JSON file handle
	unique_ptr<FileHandle> file_handle;
	Allocator &allocator;

	//! File properties
	const bool can_seek;
	const bool plain_file_source;
	const idx_t file_size;

	//! Read properties
	idx_t read_position;
	idx_t requested_reads;
	atomic<idx_t> actual_reads;

	//! Cached buffers for resetting when reading stream
	vector<AllocatedData> cached_buffers;
	idx_t cached_size;
};

class BufferedJSONReader {
public:
	BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options, string file_name);

	void OpenJSONFile();
	void CloseJSONFile();
	void Reset();

	bool HasFileHandle() const;
	bool IsOpen() const;

	BufferedJSONReaderOptions &GetOptions();

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
	AllocatedData RemoveBuffer(idx_t buffer_idx);

	//! Get a new buffer index (must hold the lock)
	idx_t GetBufferIndex();
	//! Set line count for a buffer that is done (grabs the lock)
	void SetBufferLineOrObjectCount(idx_t index, idx_t count);
	//! Throws a parse error that mentions the file name and line number
	void ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf, yyjson_read_err &err, const string &extra = "");
	//! Throws a transform error that mentions the file name and line number
	void ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf, const string &error_message);

	//! Scan progress
	double GetProgress() const;

private:
	idx_t GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf);

private:
	ClientContext &context;
	BufferedJSONReaderOptions options;

	//! File name
	const string file_name;
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
	mutex lock;
	MultiFileReaderData reader_data;
};

} // namespace duckdb
