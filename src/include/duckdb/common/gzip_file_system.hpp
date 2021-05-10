//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/gzip_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class GZipFileSystem : public FileSystem {
public:
	static unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle);

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	// unsupported operations
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void FileSync(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;

	int64_t GetFileSize(FileHandle &handle) override;

	bool OnDiskFile(FileHandle &handle) override;
	bool CanSeek() override {
		return false;
	}
};

}
