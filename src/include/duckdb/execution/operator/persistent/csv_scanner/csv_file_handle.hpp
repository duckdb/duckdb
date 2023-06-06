//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_file_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/allocator.hpp"

namespace duckdb {
class Allocator;
class FileSystem;

struct CSVFileHandle {
public:
	CSVFileHandle(FileSystem &fs, Allocator &allocator, unique_ptr<FileHandle> file_handle_p, const string &path_p,
	              FileCompressionType compression, bool enable_reset = true);

	mutex main_mutex;

public:
	bool CanSeek();
	void Seek(idx_t position);
	idx_t SeekPosition();
	void Reset();
	bool OnDiskFile();

	idx_t FileSize();

	bool FinishedReading();

	idx_t Read(void *buffer, idx_t nr_bytes);

	string ReadLine();
	void DisableReset();

	static unique_ptr<FileHandle> OpenFileHandle(FileSystem &fs, Allocator &allocator, const string &path,
	                                             FileCompressionType compression);
	static unique_ptr<CSVFileHandle> OpenFile(FileSystem &fs, Allocator &allocator, const string &path,
	                                          FileCompressionType compression, bool enable_reset);

private:
	FileSystem &fs;
	Allocator &allocator;
	unique_ptr<FileHandle> file_handle;
	string path;
	FileCompressionType compression;
	bool reset_enabled = true;
	bool can_seek = false;
	bool on_disk_file = false;
	idx_t file_size = 0;
	// reset support
	AllocatedData cached_buffer;
	idx_t read_position = 0;
	idx_t buffer_size = 0;
	idx_t buffer_capacity = 0;
	idx_t requested_bytes = 0;
};

} // namespace duckdb
