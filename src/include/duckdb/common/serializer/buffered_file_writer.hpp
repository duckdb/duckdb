//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/query_context.hpp"

namespace duckdb {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public WriteStream {
public:
	static constexpr FileOpenFlags DEFAULT_OPEN_FLAGS = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;

	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold. The optional QueryContext is used to attribute
	//! the written bytes to the query's I/O metrics.
	DUCKDB_API BufferedFileWriter(FileSystem &fs, const string &path, FileOpenFlags open_flags = DEFAULT_OPEN_FLAGS,
	                              QueryContext context = QueryContext(), idx_t buffer_size = FILE_BUFFER_SIZE);

	FileSystem &fs;
	string path;
	//! Size of the in-memory buffer (bytes) - data is flushed to the OS once it fills
	idx_t buffer_size;
	unsafe_unique_array<data_t> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;
	QueryContext context;

public:
	DUCKDB_API void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	//! Flush all changes to the file and then close the file
	DUCKDB_API void Close();
	//! Flush all changes and fsync the file to disk
	DUCKDB_API void Sync();
	//! Fsync the file to disk without flushing the in-memory buffer.
	//! Unlike the other methods, this is safe to call concurrently with WriteData/Flush from another thread:
	//! it only syncs data that was already pushed to the operating system via Flush().
	DUCKDB_API void SyncData();
	//! Flush the buffer to the file (without sync)
	DUCKDB_API void Flush();
	//! Returns the current size of the file
	DUCKDB_API idx_t GetFileSize();
	//! Truncate the size to a previous size (given that size <= GetFileSize())
	DUCKDB_API void Truncate(idx_t size);

	DUCKDB_API idx_t GetTotalWritten() const;
};

} // namespace duckdb
