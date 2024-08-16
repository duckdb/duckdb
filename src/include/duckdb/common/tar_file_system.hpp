//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/tar_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"

namespace duckdb {

class TarFileHandle final : public FileHandle {
	friend class TarFileSystem;

public:
	TarFileHandle(FileSystem &file_system, const string &path, unique_ptr<FileHandle> inner_handle_p,
	              idx_t start_offset_p, idx_t end_offset_p)
	    : FileHandle(file_system, path), inner_handle(std::move(inner_handle_p)), start_offset(start_offset_p),
	      end_offset(end_offset_p) {
	}

	void Close() override;

private:
	unique_ptr<FileHandle> inner_handle;
	idx_t start_offset;
	idx_t end_offset;
};

class TarFileSystem final : public FileSystem {
public:
	explicit TarFileSystem(VirtualFileSystem &parent_p) : FileSystem(), parent_file_system(parent_p) {
	}

	time_t GetLastModifiedTime(FileHandle &handle) override;
	FileType GetFileType(FileHandle &handle) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void Seek(FileHandle &handle, idx_t location) override;
	void Reset(FileHandle &handle) override;
	idx_t SeekPosition(FileHandle &handle) override;
	std::string GetName() const override {
		return "TarFileSystem";
	}
	vector<string> Glob(const string &path, FileOpener *opener) override;

	bool CanHandleFile(const string &fpath) override;
	bool OnDiskFile(FileHandle &handle) override;
	bool CanSeek() override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;

private:
	VirtualFileSystem &parent_file_system;
};

} // namespace duckdb
