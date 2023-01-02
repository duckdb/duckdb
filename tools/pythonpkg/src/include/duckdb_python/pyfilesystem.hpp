#pragma once

#include "duckdb/common/string_util.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb_python/python_object_container.hpp"

#include <vector>

namespace duckdb {

class AbstractFileSystem : public py::object {
public:
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return py::isinstance(object, py::module::import("fsspec").attr("AbstractFileSystem"));
	}
};

class PythonFileHandle : public FileHandle {
public:
	PythonFileHandle(FileSystem &file_system, const string &path, const py::object handle);

	void Close() override {
		PythonGILWrapper gil;
		handle.attr("close")();
	}

	static const py::object *GetHandle(const FileHandle &handle) {
		return &((PythonFileHandle &)handle).handle;
	}

private:
	py::object handle;
};
class PythonFilesystem : public FileSystem {
private:
	const vector<string> protocols;
	const string name;
	const AbstractFileSystem filesystem;
	string stripPrefix(string input) {
		for (const auto &protocol : protocols) {
			auto prefix = protocol + "://";
			if (StringUtil::StartsWith(input, prefix)) {
				return input.substr(prefix.size());
			}
		}
		return input;
	}

public:
	explicit PythonFilesystem(const vector<string> protocols, const AbstractFileSystem filesystem)
	    : protocols(protocols), name(protocols[0]), filesystem(filesystem) {
	}

protected:
	string GetName() const override {
		return name;
	}

public:
	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock,
	                                FileCompressionType compression, FileOpener *opener) override;
	void Seek(duckdb::FileHandle &handle, uint64_t location) override;
	FileType GetFileType(FileHandle &handle) override {
		return FileType::FILE_TYPE_REGULAR;
	}
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, uint64_t location) override;

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	bool FileExists(const string &filename) override;
	vector<string> Glob(const string &path, FileOpener *opener) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	int64_t GetFileSize(FileHandle &handle) override;
	void RemoveFile(const string &filename) override;
	void MoveFile(const string &source, const string &dest) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
};

} // namespace duckdb

namespace pybind11 {
namespace detail {
template <>
struct handle_type_name<duckdb::AbstractFileSystem> {
	static constexpr auto name = const_name("fsspec.AbstractFileSystem");
};
} // namespace detail
} // namespace pybind11
