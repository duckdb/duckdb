//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/virtual_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {

// bunch of wrappers to allow registering protocol handlers
class VirtualFileSystem : public FileSystem {
public:
	VirtualFileSystem();

	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK,
	                                FileCompressionType compression = FileCompressionType::UNCOMPRESSED,
	                                FileOpener *opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		handle.file_system.Read(handle, buffer, nr_bytes, location);
	};

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		handle.file_system.Write(handle, buffer, nr_bytes, location);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return handle.file_system.Read(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return handle.file_system.Write(handle, buffer, nr_bytes);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return handle.file_system.GetFileSize(handle);
	}
	time_t GetLastModifiedTime(FileHandle &handle) override {
		return handle.file_system.GetLastModifiedTime(handle);
	}
	FileType GetFileType(FileHandle &handle) override {
		return handle.file_system.GetFileType(handle);
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		handle.file_system.Truncate(handle, new_size);
	}

	void FileSync(FileHandle &handle) override {
		handle.file_system.FileSync(handle);
	}

	// need to look up correct fs for this
	bool DirectoryExists(const string &directory) override {
		return FindFileSystem(directory)->DirectoryExists(directory);
	}
	void CreateDirectory(const string &directory) override {
		FindFileSystem(directory)->CreateDirectory(directory);
	}

	void RemoveDirectory(const string &directory) override {
		FindFileSystem(directory)->RemoveDirectory(directory);
	}

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) override {
		return FindFileSystem(directory)->ListFiles(directory, callback);
	}

	void MoveFile(const string &source, const string &target) override {
		FindFileSystem(source)->MoveFile(source, target);
	}

	bool FileExists(const string &filename) override {
		return FindFileSystem(filename)->FileExists(filename);
	}

	bool IsPipe(const string &filename) override {
		return FindFileSystem(filename)->IsPipe(filename);
	}
	virtual void RemoveFile(const string &filename) override {
		FindFileSystem(filename)->RemoveFile(filename);
	}

	virtual vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
		return FindFileSystem(path)->Glob(path, opener);
	}

	void RegisterSubSystem(unique_ptr<FileSystem> fs) override {
		sub_systems.push_back(move(fs));
	}

	void UnregisterSubSystem(const string &name) {
		for (auto sub_system = sub_systems.begin(); sub_system != sub_systems.end(); sub_system++) {
			if (sub_system->get()->GetName() == name) {
				sub_systems.erase(sub_system);
				return;
			}
		}
		throw InvalidInputException("Could not find filesystem with name %s", name);
	}

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		compressed_fs[compression_type] = move(fs);
	}

	std::string GetName() const override {
		return "VirtualFileSystem";
	}

private:
	FileSystem *FindFileSystem(const string &path) {
		for (auto &sub_system : sub_systems) {
			if (sub_system->CanHandleFile(path)) {
				return sub_system.get();
			}
		}
		return default_fs.get();
	}

private:
	vector<unique_ptr<FileSystem>> sub_systems;
	map<FileCompressionType, unique_ptr<FileSystem>> compressed_fs;
	const unique_ptr<FileSystem> default_fs;
};

} // namespace duckdb
