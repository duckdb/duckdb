//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_open_flags.hpp"
#include <functional>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

namespace duckdb {
class AttachedDatabase;
class ClientContext;
class DatabaseInstance;
class FileOpener;
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
	DUCKDB_API FileHandle(FileSystem &file_system, string path, FileOpenFlags flags);
	FileHandle(const FileHandle &) = delete;
	DUCKDB_API virtual ~FileHandle();

	DUCKDB_API int64_t Read(void *buffer, idx_t nr_bytes);
	DUCKDB_API int64_t Write(void *buffer, idx_t nr_bytes);
	DUCKDB_API void Read(void *buffer, idx_t nr_bytes, idx_t location);
	DUCKDB_API void Write(void *buffer, idx_t nr_bytes, idx_t location);
	DUCKDB_API void Seek(idx_t location);
	DUCKDB_API void Reset();
	DUCKDB_API idx_t SeekPosition();
	DUCKDB_API void Sync();
	DUCKDB_API void Truncate(int64_t new_size);
	DUCKDB_API string ReadLine();
	DUCKDB_API bool Trim(idx_t offset_bytes, idx_t length_bytes);
	DUCKDB_API virtual idx_t GetProgress();
	DUCKDB_API virtual FileCompressionType GetFileCompressionType();

	DUCKDB_API bool CanSeek();
	DUCKDB_API bool IsPipe();
	DUCKDB_API bool OnDiskFile();
	DUCKDB_API idx_t GetFileSize();
	DUCKDB_API FileType GetType();

	//! Closes the file handle.
	DUCKDB_API virtual void Close() = 0;

	string GetPath() const {
		return path;
	}

	FileOpenFlags GetFlags() const {
		return flags;
	}

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	FileSystem &file_system;
	string path;
	FileOpenFlags flags;
};

class FileSystem {
public:
	DUCKDB_API virtual ~FileSystem();

public:
	DUCKDB_API static FileSystem &GetFileSystem(ClientContext &context);
	DUCKDB_API static FileSystem &GetFileSystem(DatabaseInstance &db);
	DUCKDB_API static FileSystem &Get(AttachedDatabase &db);

	DUCKDB_API virtual unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                                   optional_ptr<FileOpener> opener = nullptr);

	//! Read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Read().
	DUCKDB_API virtual void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be written. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Write().
	DUCKDB_API virtual void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
	//! amount of bytes read.
	DUCKDB_API virtual int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
	DUCKDB_API virtual int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Excise a range of the file. The OS can drop pages from the page-cache, and the file-system is free to deallocate
	//! this range (sparse file support). Reads to the range will succeed but will return undefined data.
	DUCKDB_API virtual bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes);

	//! Returns the file size of a file handle, returns -1 on error
	DUCKDB_API virtual int64_t GetFileSize(FileHandle &handle);
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	DUCKDB_API virtual time_t GetLastModifiedTime(FileHandle &handle);
	//! Returns the file type of the attached handle
	DUCKDB_API virtual FileType GetFileType(FileHandle &handle);
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	DUCKDB_API virtual void Truncate(FileHandle &handle, int64_t new_size);

	//! Check if a directory exists
	DUCKDB_API virtual bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr);
	//! Create a directory if it does not exist
	DUCKDB_API virtual void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr);
	//! Recursively remove a directory and all files in it
	DUCKDB_API virtual void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr);

	//! List files in a directory, invoking the callback method for each one with (filename, is_dir)
	DUCKDB_API virtual bool ListFiles(const string &directory,
	                                  const std::function<void(const string &, bool)> &callback,
	                                  FileOpener *opener = nullptr);

	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	DUCKDB_API virtual void MoveFile(const string &source, const string &target,
	                                 optional_ptr<FileOpener> opener = nullptr);
	//! Check if a file exists
	DUCKDB_API virtual bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Check if path is pipe
	DUCKDB_API virtual bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Remove a file from disk
	DUCKDB_API virtual void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr);
	//! Sync a file handle to disk
	DUCKDB_API virtual void FileSync(FileHandle &handle);
	//! Sets the working directory
	DUCKDB_API static void SetWorkingDirectory(const string &path);
	//! Gets the working directory
	DUCKDB_API static string GetWorkingDirectory();
	//! Gets the users home directory
	DUCKDB_API static string GetHomeDirectory(optional_ptr<FileOpener> opener);
	//! Gets the users home directory
	DUCKDB_API virtual string GetHomeDirectory();
	//! Expands a given path, including e.g. expanding the home directory of the user
	DUCKDB_API static string ExpandPath(const string &path, optional_ptr<FileOpener> opener);
	//! Expands a given path, including e.g. expanding the home directory of the user
	DUCKDB_API virtual string ExpandPath(const string &path);
	//! Returns the system-available memory in bytes. Returns DConstants::INVALID_INDEX if the system function fails.
	DUCKDB_API static optional_idx GetAvailableMemory();
	//! Returns the space available on the disk. Returns DConstants::INVALID_INDEX if the information was not available.
	DUCKDB_API static optional_idx GetAvailableDiskSpace(const string &path);
	//! Path separator for path
	DUCKDB_API virtual string PathSeparator(const string &path);
	//! Checks if path is starts with separator (i.e., '/' on UNIX '\\' on Windows)
	DUCKDB_API bool IsPathAbsolute(const string &path);
	//! Normalize an absolute path - the goal of normalizing is converting "\test.db" and "C:/test.db" into "C:\test.db"
	//! so that the database system cache can correctly
	DUCKDB_API string NormalizeAbsolutePath(const string &path);
	//! Join two paths together
	DUCKDB_API string JoinPath(const string &a, const string &path);
	//! Convert separators in a path to the local separators (e.g. convert "/" into \\ on windows)
	DUCKDB_API string ConvertSeparators(const string &path);
	//! Extract the base name of a file (e.g. if the input is lib/example.dll the base name is 'example')
	DUCKDB_API string ExtractBaseName(const string &path);
	//! Extract the name of a file (e.g if the input is lib/example.dll the name is 'example.dll')
	DUCKDB_API string ExtractName(const string &path);

	//! Returns the value of an environment variable - or the empty string if it is not set
	DUCKDB_API static string GetEnvVariable(const string &name);

	//! Whether there is a glob in the string
	DUCKDB_API static bool HasGlob(const string &str);
	//! Runs a glob on the file system, returning a list of matching files
	DUCKDB_API virtual vector<string> Glob(const string &path, FileOpener *opener = nullptr);
	DUCKDB_API vector<string> GlobFiles(const string &path, ClientContext &context,
	                                    FileGlobOptions options = FileGlobOptions::DISALLOW_EMPTY);

	//! registers a sub-file system to handle certain file name prefixes, e.g. http:// etc.
	DUCKDB_API virtual void RegisterSubSystem(unique_ptr<FileSystem> sub_fs);
	DUCKDB_API virtual void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs);

	//! Unregister a sub-filesystem by name
	DUCKDB_API virtual void UnregisterSubSystem(const string &name);

	//! List registered sub-filesystems, including builtin ones
	DUCKDB_API virtual vector<string> ListSubSystems();

	//! Whether or not a sub-system can handle a specific file path
	DUCKDB_API virtual bool CanHandleFile(const string &fpath);

	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	DUCKDB_API virtual void Seek(FileHandle &handle, idx_t location);
	//! Reset a file to the beginning (equivalent to Seek(handle, 0) for simple files)
	DUCKDB_API virtual void Reset(FileHandle &handle);
	DUCKDB_API virtual idx_t SeekPosition(FileHandle &handle);

	//! If FS was manually set by the user
	DUCKDB_API virtual bool IsManuallySet();
	//! Whether or not we can seek into the file
	DUCKDB_API virtual bool CanSeek();
	//! Whether or not the FS handles plain files on disk. This is relevant for certain optimizations, as random reads
	//! in a file on-disk are much cheaper than e.g. random reads in a file over the network
	DUCKDB_API virtual bool OnDiskFile(FileHandle &handle);

	DUCKDB_API virtual unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle, bool write);

	//! Create a LocalFileSystem.
	DUCKDB_API static unique_ptr<FileSystem> CreateLocal();

	//! Return the name of the filesytem. Used for forming diagnosis messages.
	DUCKDB_API virtual std::string GetName() const = 0;

	//! Whether or not a file is remote or local, based only on file path
	DUCKDB_API static bool IsRemoteFile(const string &path);
	DUCKDB_API static bool IsRemoteFile(const string &path, string &extension);

	DUCKDB_API virtual void SetDisabledFileSystems(const vector<string> &names);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
