#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pybind11/gil_wrapper.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ModifiedMemoryFileSystem : public py::object {
public:
	using py::object::object;
	ModifiedMemoryFileSystem(py::object object) : py::object(object) {
	}

public:
	static bool check_(const py::handle &object) {
		return py::isinstance(object, py::module::import("duckdb.filesystem").attr("ModifiedMemoryFileSystem"));
	}
};

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
	PythonFileHandle(FileSystem &file_system, const string &path, const py::object &handle);
	~PythonFileHandle() override;
	void Close() override {
		PythonGILWrapper gil;
		handle.attr("close")();
	}

	static const py::object &GetHandle(const FileHandle &handle) {
		return ((const PythonFileHandle &)handle).handle;
	}

private:
	py::object handle;
};
class PythonFilesystem : public FileSystem {
private:
	const vector<string> protocols;
	AbstractFileSystem filesystem;
	std::string DecodeFlags(FileOpenFlags flags);
	bool Exists(const string &filename, const char *func_name) const;

public:
	explicit PythonFilesystem(vector<string> protocols, AbstractFileSystem filesystem)
	    : protocols(std::move(protocols)), filesystem(std::move(filesystem)) {
	}
	~PythonFilesystem() override;

protected:
	string GetName() const override {
		return protocols[0];
	}

public:
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	void Seek(duckdb::FileHandle &handle, uint64_t location) override;
	FileType GetFileType(FileHandle &handle) override {
		return FileType::FILE_TYPE_REGULAR;
	}
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, uint64_t location) override;

	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	vector<string> Glob(const string &path, FileOpener *opener) override;
	bool CanHandleFile(const string &fpath) override;
	bool CanSeek() override {
		return true;
	}

	bool IsManuallySet() override {
		return true;
	}

	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}
	string PathSeparator(const string &path) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void MoveFile(const string &source, const string &dest, optional_ptr<FileOpener> opener = nullptr) override;
	time_t GetLastModifiedTime(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	idx_t SeekPosition(FileHandle &handle) override;
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
