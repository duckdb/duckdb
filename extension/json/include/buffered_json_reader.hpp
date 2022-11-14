//===----------------------------------------------------------------------===//
//                         DuckDB
//
// buffered_json_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/file_compression_type.hpp"
#include "json_common.hpp"

namespace duckdb {

struct FileHandle;

enum class JSONFormat : uint8_t {
	//! Auto-detect during the bind (TODO)
	AUTO_DETECT = 0,
	//! One object after another, newlines can be anywhere
	UNSTRUCTURED = 1,
	//! Objects are separated by newlines, newlines do not occur within objects (NDJSON)
	NEWLINE_DELIMITED = 2,
};

struct BufferedJSONReaderOptions {
	//! The file path of the JSON file to read
	string file_path;
	//! The format of the JSON
	JSONFormat format = JSONFormat::AUTO_DETECT;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 1MB)
	idx_t maximum_object_size = 1048576;
};

struct JSONFileHandle {
public:
	explicit JSONFileHandle(unique_ptr<FileHandle> file_handle);

	idx_t FileSize() const;
	idx_t Remaining() const;
	bool CanSeek() const;

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
	BufferedJSONReader(ClientContext &context, BufferedJSONReaderOptions options);
	void OpenJSONFile();
	JSONFileHandle &GetFileHandle();
	double GetProgress() const;
	idx_t MaxThreads(idx_t buffer_capacity) const;

public:
	BufferedJSONReaderOptions options;

private:
	ClientContext &context;

	//! The file currently being read
	unique_ptr<JSONFileHandle> file_handle;
};

} // namespace duckdb
