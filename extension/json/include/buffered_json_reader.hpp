//===----------------------------------------------------------------------===//
//                         DuckDB
//
// buffered_json_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "json_common.hpp"

namespace duckdb {

struct FileHandle;

enum class JSONFormat : uint8_t {
	//! Auto-detect format (UNSTRUCTURED / NEWLINE_DELIMITED)
	AUTO_DETECT = 0,
	//! One object after another, newlines can be anywhere
	UNSTRUCTURED = 1,
	//! Objects are separated by newlines, newlines do not occur within objects (NDJSON)
	NEWLINE_DELIMITED = 2,
};

struct BufferedJSONReaderOptions {
public:
	//! The file path of the JSON file to read
	string file_path;
	//! The format of the JSON
	JSONFormat format = JSONFormat::AUTO_DETECT;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;

public:
	void Serialize(FieldWriter &writer);
	void Deserialize(FieldReader &reader);
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
	explicit JSONFileHandle(unique_ptr<FileHandle> file_handle);

	idx_t FileSize() const;
	idx_t Remaining() const;

	bool CanSeek() const;
	bool PlainFileSource() const;

	idx_t GetPositionAndSize(idx_t &position, idx_t requested_size);
	void ReadAtPosition(const char *pointer, idx_t size, idx_t position);
	idx_t Read(const char *pointer, idx_t requested_size);

private:
	//! The JSON file handle
	unique_ptr<FileHandle> file_handle;

	//! File properties
	const bool can_seek;
	const bool plain_file_source;
	const idx_t file_size;

	//! Read properties
	idx_t read_position;
};

class BufferedJSONReader {
public:
	BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options, idx_t file_index, string file_path);

	void OpenJSONFile();
	bool IsOpen();

	BufferedJSONReaderOptions &GetOptions();
	JSONFileHandle &GetFileHandle() const;

	void InsertBuffer(idx_t buffer_idx, unique_ptr<JSONBufferHandle> &&buffer);
	JSONBufferHandle *GetBuffer(idx_t buffer_idx);
	AllocatedData RemoveBuffer(idx_t buffer_idx);
	idx_t GetBufferIndex();

	double GetProgress() const;

public:
	mutex lock;

	//! File index / path
	const idx_t file_index;
	const string file_path;

private:
	ClientContext &context;
	BufferedJSONReaderOptions options;

	//! File handle
	unique_ptr<JSONFileHandle> file_handle;

	//! Next buffer index within the file
	idx_t buffer_index;
	//! Mapping from batch index to currently held buffers
	unordered_map<idx_t, unique_ptr<JSONBufferHandle>> buffer_map;
};

} // namespace duckdb
