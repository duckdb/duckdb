//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "common/file_buffer.hpp"

#include <functional>

namespace duckdb {
class FileSystem;

struct FileHandle {
public:
	FileHandle(FileSystem &file_system, string path) : file_system(file_system), path(path) {
	}
	FileHandle(const FileHandle &) = delete;
	virtual ~FileHandle() {
	}

	void Read(void *buffer, count_t nr_bytes, index_t location);
	void Write(void *buffer, count_t nr_bytes, index_t location);
	void Sync();

protected:
	virtual void Close() = 0;

public:
	FileSystem &file_system;
	string path;
};

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

class FileFlags {
public:
	//! Open file with read access
	static constexpr uint8_t READ = 1 << 0;
	//! Open file with read/write access
	static constexpr uint8_t WRITE = 1 << 1;
	//! Use direct IO when reading/writing to the file
	static constexpr uint8_t DIRECT_IO = 1 << 2;
	//! Create file if not exists, can only be used together with WRITE
	static constexpr uint8_t CREATE = 1 << 3;
};

class FileSystem {
public:
	virtual unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK);
	unique_ptr<FileHandle> OpenFile(string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK) {
		return OpenFile(path.c_str(), flags, lock);
	}
	virtual void Read(FileHandle &handle, void *buffer, index_t nr_bytes, index_t location);
	virtual void Write(FileHandle &handle, void *buffer, index_t nr_bytes, index_t location);

	//! Check if a directory exists
	virtual bool DirectoryExists(const string &directory);
	//! Create a directory if it does not exist
	virtual void CreateDirectory(const string &directory);
	//! Recursively remove a directory and all files in it
	virtual void RemoveDirectory(const string &directory);
	//! List files in a directory, invoking the callback method for each one
	virtual bool ListFiles(const string &directory, std::function<void(string)> callback);
	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	virtual void MoveFile(const string &source, const string &target);
	//! Check if a file exists
	virtual bool FileExists(const string &filename);
	//! Remove a file from disk
	virtual void RemoveFile(const string &filename);
	//! Path separator for the current file system
	virtual string PathSeparator();
	//! Join two paths together
	virtual string JoinPath(const string &a, const string &path);
	//! Sync a file descriptor to disk
	virtual void FileSync(FILE *file);
	//! Sync a file handle to disk
	virtual void FileSync(FileHandle &handle);

	virtual ~FileSystem() {
	}
};

} // namespace duckdb
