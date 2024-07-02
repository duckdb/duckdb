//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_file_handle.hpp
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
	              FileCompressionType compression);

	mutex main_mutex;

	bool CanSeek();
	void Seek(idx_t position);
	bool OnDiskFile();
	bool IsPipe();

	void Reset();

	idx_t FileSize();

	bool FinishedReading();

	idx_t Read(void *buffer, idx_t nr_bytes);

	string ReadLine();

	string GetFilePath();

	static unique_ptr<FileHandle> OpenFileHandle(FileSystem &fs, Allocator &allocator, const string &path,
	                                             FileCompressionType compression);
	static unique_ptr<CSVFileHandle> OpenFile(FileSystem &fs, Allocator &allocator, const string &path,
	                                          FileCompressionType compression);
	FileCompressionType compression_type;

	double GetProgress();

private:
	unique_ptr<FileHandle> file_handle;
	string path;
	bool can_seek = false;
	bool on_disk_file = false;
	bool is_pipe = false;
	idx_t uncompressed_bytes_read = 0;

	idx_t file_size = 0;

	idx_t requested_bytes = 0;
	//! If we finished reading the file
	bool finished = false;
};

} // namespace duckdb
