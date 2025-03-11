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
#include "duckdb/execution/operator/csv_scanner/encode/csv_encoder.hpp"
namespace duckdb {
class Allocator;
class FileSystem;
struct CSVReaderOptions;

class CSVFileHandle {
public:
	CSVFileHandle(DBConfig &config, unique_ptr<FileHandle> file_handle_p, const string &path_p,
	              const CSVReaderOptions &options);

	mutex main_mutex;

	bool CanSeek() const;
	void Seek(idx_t position) const;
	bool OnDiskFile() const;
	bool IsPipe() const;

	void Reset();

	idx_t FileSize() const;

	bool FinishedReading() const;

	idx_t Read(void *buffer, idx_t nr_bytes);

	string ReadLine();

	string GetFilePath();

	static unique_ptr<FileHandle> OpenFileHandle(FileSystem &fs, Allocator &allocator, const string &path,
	                                             FileCompressionType compression);
	static unique_ptr<CSVFileHandle> OpenFile(DBConfig &config, FileSystem &fs, Allocator &allocator,
	                                          const string &path, const CSVReaderOptions &options);
	FileCompressionType compression_type;

	double GetProgress() const;

private:
	unique_ptr<FileHandle> file_handle;
	CSVEncoder encoder;
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
