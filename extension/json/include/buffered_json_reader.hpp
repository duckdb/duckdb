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

struct BufferedJSONReaderOptions {
	//! The file path of the JSON file to read
	string file_path;
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

	idx_t Remaining() const;
	idx_t GetPositionAndSize(idx_t &position, idx_t requested_size);
	void Read(data_ptr_t pointer, idx_t size, idx_t position);

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

private:
	ClientContext &context;
	BufferedJSONReaderOptions options;

	//! The file currently being read
	unique_ptr<JSONFileHandle> file_handle;
};

} // namespace duckdb
