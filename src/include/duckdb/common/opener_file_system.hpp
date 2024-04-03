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

	void VerifyNoOpener(optional_ptr<FileOpener> opener) {
		if (opener) {
			throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
		}
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		VerifyNoOpener(opener);
		return GetFileSystem().OpenFile(path, flags, GetOpener());
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

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return GetFileSystem().DirectoryExists(directory, GetOpener());
	}
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return GetFileSystem().CreateDirectory(directory, GetOpener());
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return GetFileSystem().RemoveDirectory(directory, GetOpener());
	}

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		VerifyNoOpener(opener);
		return GetFileSystem().ListFiles(directory, callback, GetOpener().get());
	}

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		GetFileSystem().MoveFile(source, target, GetOpener());
	}

	string GetHomeDirectory() override {
		return FileSystem::GetHomeDirectory(GetOpener());
	}

	string ExpandPath(const string &path) override {
		return FileSystem::ExpandPath(path, GetOpener());
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return GetFileSystem().FileExists(filename, GetOpener());
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		return GetFileSystem().IsPipe(filename, GetOpener());
	}
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener) override {
		VerifyNoOpener(opener);
		GetFileSystem().RemoveFile(filename, GetOpener());
	}

	string PathSeparator(const string &path) override {
		return GetFileSystem().PathSeparator(path);
	}

	vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		VerifyNoOpener(opener);
		return GetFileSystem().Glob(path, GetOpener().get());
	}

	std::string GetName() const override {
		return "OpenerFileSystem - " + GetFileSystem().GetName();
	}

	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		GetFileSystem().RegisterSubSystem(std::move(sub_fs));
	}

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		GetFileSystem().RegisterSubSystem(compression_type, std::move(fs));
	}

	void UnregisterSubSystem(const string &name) override {
		GetFileSystem().UnregisterSubSystem(name);
	}

	void SetDisabledFileSystems(const vector<string> &names) override {
		GetFileSystem().SetDisabledFileSystems(names);
	}

	vector<string> ListSubSystems() override {
		return GetFileSystem().ListSubSystems();
	}
};

} // namespace duckdb
