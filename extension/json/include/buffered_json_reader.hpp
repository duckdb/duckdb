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

class FileOpener;
class FileSystem;

struct BufferedJSONReaderOptions {
	//! The file path of the JSON file to read
	string file_path;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 2MB)
	idx_t maximum_object_size = 2097152;
};

struct JSONFileHandle {
public:
	explicit JSONFileHandle(unique_ptr<FileHandle> file_handle);

	idx_t Remaining() const;
	void Read(data_ptr_t pointer, idx_t size);

private:
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

	void Initialize();

	AllocatedData AllocateBuffer();

private:
	void OpenJSONFile();

private:

	//! Initial buffer capacity (4MB)
	static constexpr idx_t INITIAL_BUFFER_CAPACITY = 4194304;

	//! The file currently being read
	unique_ptr<JSONFileHandle> file_handle;
	//! The current block capacity
	idx_t buffer_capacity;

private:
	ClientContext &context;
	BufferedJSONReaderOptions options;

	Allocator &allocator;
	FileSystem &file_system;
};

} // namespace duckdb
