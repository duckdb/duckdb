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

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	void Reset(FileHandle &handle) override;

	int64_t GetFileSize(FileHandle &handle) override;

	bool OnDiskFile(FileHandle &handle) override;
	bool CanSeek() override {
		return false;
	}

	std::string GetName() const override {
		return "GZipFileSystem";
	}
};

} // namespace duckdb
