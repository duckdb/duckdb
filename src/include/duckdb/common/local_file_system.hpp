//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/local_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class LocalFileSystem : public FileSystem {
public:
	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK,
	                                FileCompressionType compression = FileCompressionType::UNCOMPRESSED,
	                                FileOpener *opener = nullptr) override;

	//! Read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Read().
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	//! Write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be written. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Write().
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	//! Read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
	//! amount of bytes read.
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	//! Write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	//! Returns the file size of a file handle, returns -1 on error
	int64_t GetFileSize(FileHandle &handle) override;
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	time_t GetLastModifiedTime(FileHandle &handle) override;
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	FileType GetFileType(FileHandle &handle) override;
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	void Truncate(FileHandle &handle, int64_t new_size) override;

	//! Check if a directory exists
	bool DirectoryExists(const string &directory) override;
	//! Create a directory if it does not exist
	void CreateDirectory(const string &directory) override;
	//! Recursively remove a directory and all files in it
	void RemoveDirectory(const string &directory) override;
	//! List files in a directory, invoking the callback method for each one with (filename, is_dir)
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;
	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	void MoveFile(const string &source, const string &target) override;
	//! Check if a file exists
	bool FileExists(const string &filename) override;

	//! Check if path is a pipe
	bool IsPipe(const string &filename) override;
	//! Remove a file from disk
	void RemoveFile(const string &filename) override;
	//! Sync a file handle to disk
	void FileSync(FileHandle &handle) override;

	//! Runs a glob on the file system, returning a list of matching files
	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override;

	bool CanHandleFile(const string &fpath) override {
		//! Whether or not a sub-system can handle a specific file path
		return false;
	}

	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void Seek(FileHandle &handle, idx_t location) override;
	//! Return the current seek posiiton in the file.
	idx_t SeekPosition(FileHandle &handle) override;

	//! Whether or not we can seek into the file
	bool CanSeek() override;
	//! Whether or not the FS handles plain files on disk. This is relevant for certain optimizations, as random reads
	//! in a file on-disk are much cheaper than e.g. random reads in a file over the network
	bool OnDiskFile(FileHandle &handle) override;

	std::string GetName() const override {
		return "LocalFileSystem";
	}

	//! Returns the last Win32 error, in string format. Returns an empty string if there is no error, or on non-Windows
	//! systems.
	static std::string GetLastErrorAsString();

private:
	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void SetFilePointer(FileHandle &handle, idx_t location);
	idx_t GetFilePointer(FileHandle &handle);

	vector<string> FetchFileWithoutGlob(const string &path, FileOpener *opener, bool absolute_path);
};

} // namespace duckdb
