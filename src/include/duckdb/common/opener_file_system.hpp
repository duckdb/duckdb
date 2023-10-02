//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/opener_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

// The OpenerFileSystem is wrapper for a file system that pushes an appropriate FileOpener into the various API calls
class OpenerFileSystem : public FileSystem {
public:
	virtual FileSystem &GetFileSystem() const = 0;
	virtual optional_ptr<FileOpener> GetOpener() const = 0;

	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK,
	                                FileCompressionType compression = FileCompressionType::UNCOMPRESSED,
	                                FileOpener *opener = nullptr) override {
		if (opener) {
			throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
		}
		return GetFileSystem().OpenFile(path, flags, lock, compression, GetOpener().get());
	}

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		GetFileSystem().Read(handle, buffer, nr_bytes, location);
	};

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		GetFileSystem().Write(handle, buffer, nr_bytes, location);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return GetFileSystem().Read(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return GetFileSystem().Write(handle, buffer, nr_bytes);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return GetFileSystem().GetFileSize(handle);
	}
	time_t GetLastModifiedTime(FileHandle &handle) override {
		return GetFileSystem().GetLastModifiedTime(handle);
	}
	FileType GetFileType(FileHandle &handle) override {
		return GetFileSystem().GetFileType(handle);
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		GetFileSystem().Truncate(handle, new_size);
	}

	void FileSync(FileHandle &handle) override {
		GetFileSystem().FileSync(handle);
	}

	bool DirectoryExists(const string &directory) override {
		return GetFileSystem().DirectoryExists(directory);
	}
	void CreateDirectory(const string &directory) override {
		return GetFileSystem().CreateDirectory(directory);
	}

	void RemoveDirectory(const string &directory) override {
		return GetFileSystem().RemoveDirectory(directory);
	}

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		if (opener) {
			throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
		}
		return GetFileSystem().ListFiles(directory, callback, GetOpener().get());
	}

	void MoveFile(const string &source, const string &target) override {
		GetFileSystem().MoveFile(source, target);
	}

	string GetHomeDirectory() override {
		return FileSystem::GetHomeDirectory(GetOpener());
	}

	string ExpandPath(const string &path) override {
		return FileSystem::ExpandPath(path, GetOpener());
	}

	bool FileExists(const string &filename) override {
		return GetFileSystem().FileExists(filename);
	}

	bool IsPipe(const string &filename) override {
		return GetFileSystem().IsPipe(filename);
	}
	void RemoveFile(const string &filename) override {
		GetFileSystem().RemoveFile(filename);
	}

	string PathSeparator(const string &path) override {
		return GetFileSystem().PathSeparator(path);
	}

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		if (opener) {
			throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
		}
		return GetFileSystem().Glob(path, GetOpener().get());
	}

	std::string GetName() const override {
		return "OpenerFileSystem - " + GetFileSystem().GetName();
	}
};

} // namespace duckdb
