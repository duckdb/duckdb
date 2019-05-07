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

struct FileHandle {
public:
	FileHandle(string path) : path(path) {
	}
	FileHandle(const FileHandle &) = delete;
	virtual ~FileHandle() {
	}

	void Read(void *buffer, uint64_t nr_bytes, uint64_t location);
	void Write(void *buffer, uint64_t nr_bytes, uint64_t location);
	void Sync();
protected:
	virtual void Close() = 0;

public:
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
	static unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK);
	static unique_ptr<FileHandle> OpenFile(string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK) {
		return OpenFile(path.c_str(), flags, lock);
	}
	static void Read(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location);
	static void Write(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location);

	//! Check if a directory exists
	static bool DirectoryExists(const string &directory);
	//! Create a directory if it does not exist
	static void CreateDirectory(const string &directory);
	//! Recursively remove a directory and all files in it
	static void RemoveDirectory(const string &directory);
	//! List files in a directory, invoking the callback method for each one
	static bool ListFiles(const string &directory, std::function<void(string)> callback);
	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	static void MoveFile(const string &source, const string &target);
	//! Check if a file exists
	static bool FileExists(const string &filename);
	//! Remove a file from disk
	static void RemoveFile(const string &filename);
	//! Path separator for the current file system
	static string PathSeparator();
	//! Join two paths together
	static string JoinPath(const string &a, const string &path);
	//! Sync a file descriptor to disk
	static void FileSync(FILE *file);
	//! Sync a file handle to disk
	static void FileSync(FileHandle &handle);
};

} // namespace duckdb
