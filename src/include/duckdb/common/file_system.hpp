//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"

#include <functional>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

namespace duckdb {
class ClientContext;
class DatabaseInstance;
class FileSystem;

enum class FileType {
	//! Regular file
	FILE_TYPE_REGULAR,
	//! Directory
	FILE_TYPE_DIR,
	//! FIFO named pipe
	FILE_TYPE_FIFO,
	//! Socket
	FILE_TYPE_SOCKET,
	//! Symbolic link
	FILE_TYPE_LINK,
	//! Block device
	FILE_TYPE_BLOCKDEV,
	//! Character device
	FILE_TYPE_CHARDEV,
	//! Unknown or invalid file handle
	FILE_TYPE_INVALID,
};

struct FileHandle {
public:
	FileHandle(FileSystem &file_system, string path) : file_system(file_system), path(path) {
	}
	FileHandle(const FileHandle &) = delete;
	virtual ~FileHandle() {
	}

	int64_t Read(void *buffer, idx_t nr_bytes);
	int64_t Write(void *buffer, idx_t nr_bytes);
	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	void Seek(idx_t location);
	void Reset();
	idx_t SeekPosition();
	void Sync();
	void Truncate(int64_t new_size);
	string ReadLine();

	bool CanSeek();
	bool OnDiskFile();
	idx_t GetFileSize();
	FileType GetType();

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
	static constexpr uint8_t FILE_FLAGS_READ = 1 << 0;
	//! Open file with read/write access
	static constexpr uint8_t FILE_FLAGS_WRITE = 1 << 1;
	//! Use direct IO when reading/writing to the file
	static constexpr uint8_t FILE_FLAGS_DIRECT_IO = 1 << 2;
	//! Create file if not exists, can only be used together with WRITE
	static constexpr uint8_t FILE_FLAGS_FILE_CREATE = 1 << 3;
	//! Always create a new file. If a file exists, the file is truncated. Cannot be used together with CREATE.
	static constexpr uint8_t FILE_FLAGS_FILE_CREATE_NEW = 1 << 4;
	//! Open file in append mode
	static constexpr uint8_t FILE_FLAGS_APPEND = 1 << 5;
};

class FileSystem {
public:
	virtual ~FileSystem() {
	}

public:
	static FileSystem &GetFileSystem(ClientContext &context);
	static FileSystem &GetFileSystem(DatabaseInstance &db);

	virtual unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags,
	                                        FileLockType lock = FileLockType::NO_LOCK,
	                                        FileCompressionType compression = FileCompressionType::UNCOMPRESSED);

	//! Read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Read().
	virtual void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Write().
	virtual void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
	//! amount of bytes read.
	virtual int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
	virtual int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);

	//! Returns the file size of a file handle, returns -1 on error
	virtual int64_t GetFileSize(FileHandle &handle);
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	virtual time_t GetLastModifiedTime(FileHandle &handle);
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	virtual FileType GetFileType(FileHandle &handle);
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	virtual void Truncate(FileHandle &handle, int64_t new_size);

	//! Check if a directory exists
	virtual bool DirectoryExists(const string &directory);
	//! Create a directory if it does not exist
	virtual void CreateDirectory(const string &directory);
	//! Recursively remove a directory and all files in it
	virtual void RemoveDirectory(const string &directory);
	//! List files in a directory, invoking the callback method for each one with (filename, is_dir)
	virtual bool ListFiles(const string &directory, const std::function<void(string, bool)> &callback);
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
	//! Convert separators in a path to the local separators (e.g. convert "/" into \\ on windows)
	virtual string ConvertSeparators(const string &path);
	//! Extract the base name of a file (e.g. if the input is lib/example.dll the base name is example)
	virtual string ExtractBaseName(const string &path);
	//! Sync a file handle to disk
	virtual void FileSync(FileHandle &handle);

	// TODO(omo): Consider making them static. These aren't actually a FS concept but process properties.
	//! Sets the working directory
	virtual void SetWorkingDirectory(const string &path);
	//! Gets the working directory
	virtual string GetWorkingDirectory();
	//! Gets the users home directory
	virtual string GetHomeDirectory();
	//! Returns the system-available memory in bytes
	virtual idx_t GetAvailableMemory();

	//! Runs a glob on the file system, returning a list of matching files
	virtual vector<string> Glob(const string &path);

	//! registers a sub-file system to handle certain file name prefixes, e.g. http:// etc.
	virtual void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
		throw NotImplementedException("Can't register a sub system on a non-virtual file system");
	}

	virtual bool CanHandleFile(const string &fpath) {
		//! Whether or not a sub-system can handle a specific file path
		return false;
	}

	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	virtual void Seek(FileHandle &handle, idx_t location);
	//! Reset a file to the beginning (equivalent to Seek(handle, 0) for simple files)
	virtual void Reset(FileHandle &handle);
	virtual idx_t SeekPosition(FileHandle &handle);

	//! Whether or not we can seek into the file
	virtual bool CanSeek();
	//! Whether or not the FS handles plain files on disk. This is relevant for certain optimizations, as random reads
	//! in a file on-disk are much cheaper than e.g. random reads in a file over the network
	virtual bool OnDiskFile(FileHandle &handle);

	//! Create a LocalFileSystem.
	static unique_ptr<FileSystem> CreateLocal();

private:
	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void SetFilePointer(FileHandle &handle, idx_t location);
	virtual idx_t GetFilePointer(FileHandle &handle);
};

} // namespace duckdb
