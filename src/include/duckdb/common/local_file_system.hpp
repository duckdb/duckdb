//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/local_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/base_file_system.hpp"
#include "duckdb/common/windows_undefs.hpp"

namespace duckdb {

class LocalFileSystem : public BaseFileSystem {
public:
	unique_ptr<FileHandle> OpenFile(Context &context, const string &path, FileOpenFlags flags) override;

	unique_ptr<MemoryMappedFile> MemoryMapFile(Context &context, const OpenFileInfo &path, FileOpenFlags flags,
	                                           const MMapOptions &options) override;

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
	FileWriteMode GetWriteMode(FileHandle &handle) override;
	//! Excise a range of the file. The file-system is free to deallocate this
	//! range (sparse file support). Reads to the range will succeed but will return
	//! undefined data.
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;

	//! Returns the file size of a file handle, returns -1 on error
	int64_t GetFileSize(FileHandle &handle) override;
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	timestamp_t GetLastModifiedTime(FileHandle &handle) override;
	//! Returns a tag that uniquely identifies the version of the file
	string GetVersionTag(FileHandle &handle) override;
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	FileType GetFileType(FileHandle &handle) override;
	//! Returns the file stats of the attached handle.
	FileMetadata Stats(FileHandle &handle) override;
	optional<FileMetadata> GetStatsIfExists(Context &context, const OpenFileInfo &file) override;
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	void Truncate(FileHandle &handle, int64_t new_size) override;

	//! Check if a directory exists
	bool DirectoryExists(Context &context, const string &directory) override;
	//! Create a directory if it does not exist
	void CreateDirectory(Context &context, const string &directory) override;
	bool CreateDirectoryExtended(Context &context, const string &directory,
	                             const CreateDirectoryOptions &options) override;
	void CreateDirectoriesRecursive(Context &context, const string &path) override;
	//! Recursively remove a directory and all files in it
	void RemoveDirectory(Context &context, const string &directory) override;
	bool RemoveDirectoryExtended(Context &context, const string &directory,
	                             const RemoveDirectoryOptions &options) override;
	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	void MoveFile(Context &context, const string &source, const string &target) override;
	//! Check if a file exists
	bool FileExists(Context &context, const string &filename) override;

	//! Check if path is a pipe or character device
	bool IsPipe(Context &context, const string &filename) override;
	//! Remove a file from disk
	void RemoveFile(Context &context, const string &filename) override;
	//! Sync a file handle to disk
	void FileSync(FileHandle &handle) override;
	//! Close and remove an incomplete output file
	void AbortFileWrite(FileHandle &handle) override;

	//! Checks if path is is an absolute path
	bool IsPathAbsolute(const string &path) override;
	string MakePathAbsolute(const string &input, const string &home_directory);
	bool PathStartsWithDrive(const string &path);

	bool CanHandleFile(const string &fpath) override {
		//! Whether or not a sub-system can handle a specific file path
		return false;
	}

	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void Seek(FileHandle &handle, idx_t location) override;
	//! Return the current seek position in the file.
	idx_t SeekPosition(FileHandle &handle) override;

	//! Whether or not we can seek into the file
	bool CanSeek() override;
	//! Whether or not the FS handles plain files on disk. This is relevant for certain optimizations, as random reads
	//! in a file on-disk are much cheaper than e.g. random reads in a file over the network
	bool OnDiskFile(FileHandle &handle) override;

	bool IsLocalFileSystem() const override {
		return true;
	}

	std::string GetName() const override {
		return "LocalFileSystem";
	}

	//! Returns the last Win32 error, in string format. Returns an empty string if there is no error, or on non-Windows
	//! systems.
	static std::string GetLastErrorAsString();

	//! Checks a file is private (checks for 600 on linux/macos, TODO: currently always returns true on windows)
	static bool IsPrivateFile(const string &path_p);

	vector<OpenFileInfo> FetchFileWithoutGlob(const string &path, Context &context, bool absolute_path);

	string CanonicalizePath(Context &context, const string &path_p) override;
	//! Canonicalizes a path, expanding "~" with the given home directory
	string CanonicalizePathWithHome(const string &path_p, const string &home_directory);

protected:
	bool ListFilesExtended(Context &context, const string &directory,
	                       const std::function<void(OpenFileInfo &info)> &callback) override;

	bool SupportsListFilesExtended() const override {
		return true;
	}

	unique_ptr<MultiFileList> GlobFilesExtended(Context &context, const string &path,
	                                            const FileGlobInput &input) override;
	bool SupportsGlobExtended() const override {
		return true;
	}

	bool TryCanonicalizeExistingPath(string &path_p);

	string VersionTagFromMetadata(const FileMetadata &file_metadata);
	void FillFileOptions(const FileMetadata &file_metadata, unordered_map<string, Value> &options);

private:
	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void SetFilePointer(FileHandle &handle, idx_t location);
	idx_t GetFilePointer(FileHandle &handle);
};

} // namespace duckdb
