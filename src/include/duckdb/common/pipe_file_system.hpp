//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/pipe_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class PipeFileSystem : public FileSystem {
public:
	static unique_ptr<FileHandle> OpenPipe(QueryContext context, unique_ptr<FileHandle> handle);

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	int64_t GetFileSize(FileHandle &handle) override;
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;

	void Reset(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	};
	bool CanSeek() override {
		return false;
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		return true;
	}
	void FileSync(FileHandle &handle) override;

	std::string GetName() const override {
		return "PipeFileSystem";
	}
};

} // namespace duckdb
