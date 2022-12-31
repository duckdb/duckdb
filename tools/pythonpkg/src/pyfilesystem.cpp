#include "duckdb_python/pyfilesystem.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb_python/python_object_container.hpp"

namespace duckdb {

PythonFileHandle::PythonFileHandle(FileSystem &file_system, const string &path, const py::object handle)
    : FileHandle(file_system, path), handle(handle) {
}

unique_ptr<FileHandle> PythonFilesystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                  FileCompressionType compression, FileOpener *opener) {
	PythonGILWrapper gil;

	if (compression != FileCompressionType::UNCOMPRESSED) {
		throw IOException("Compression not supported");
	}

	// TODO: lock support?

	string flags_s;
	if (flags & FileFlags::FILE_FLAGS_READ) {
		flags_s = "rb";
	} else if (flags & FileFlags::FILE_FLAGS_WRITE) {
		flags_s = "wb";
	} else if (flags & FileFlags::FILE_FLAGS_APPEND) {
		flags_s = "ab";
	} else {
		throw InvalidInputException("%s: unsupported file flags", GetName());
	}

	const auto &handle = filesystem.attr("open")(py::str(stripPrefix(path)), py::str(flags_s));
	return make_unique<PythonFileHandle>(*this, path, handle);
}

int64_t PythonFilesystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	PythonGILWrapper gil;

	const auto &write = PythonFileHandle::GetHandle(handle)->attr("write");

	auto data = py::bytes(std::string(reinterpret_cast<char const *>(buffer), nr_bytes));

	return py::int_(write(data));
}
void PythonFilesystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	Seek(handle, location);

	Write(handle, buffer, nr_bytes);
}

int64_t PythonFilesystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	PythonGILWrapper gil;

	const auto &read = PythonFileHandle::GetHandle(handle)->attr("read");

	string data = py::bytes(read(nr_bytes));

	memcpy(buffer, data.c_str(), data.size());

	return data.size();
}

void PythonFilesystem::Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, uint64_t location) {
	Seek(handle, location);

	Read(handle, buffer, nr_bytes);
}
bool PythonFilesystem::FileExists(const string &filename) {
	PythonGILWrapper gil;

	return py::bool_(filesystem.attr("exists")(filename));
}
vector<string> PythonFilesystem::Glob(const string &path, FileOpener *opener) {
	PythonGILWrapper gil;

	if (!path.size()) {
		return {path};
	}
	auto returner = py::list(filesystem.attr("glob")(py::str(stripPrefix(path))));

	std::vector<string> results;
	for (auto item : returner) {
		string res = py::str(item);
		// TODO: should this slash be replaced with AbstractFileSystem#root_marker
		results.push_back(protocols[0] + "://" + "/" + res);
	}
	return results;
}
int64_t PythonFilesystem::GetFileSize(FileHandle &handle) {
	// TODO: this value should be cached on the PythonFileHandle
	PythonGILWrapper gil;

	return py::int_(PythonFileHandle::GetHandle(handle)->attr("size"));
}
void PythonFilesystem::Seek(duckdb::FileHandle &handle, uint64_t location) {
	PythonGILWrapper gil;

	auto seek = PythonFileHandle::GetHandle(handle)->attr("seek");
	seek(location);
}
bool PythonFilesystem::CanHandleFile(const string &fpath) {
	for (const auto &protocol : protocols) {
		if (StringUtil::StartsWith(fpath, protocol + "://")) {
			return true;
		}
	}
	return false;
}
void PythonFilesystem::MoveFile(const string &source, const string &dest) {
	PythonGILWrapper gil;

	auto move = filesystem.attr("mv");
	move(py::str(source), py::str(dest));
}
void PythonFilesystem::RemoveFile(const string &filename) {
	PythonGILWrapper gil;

	auto remove = filesystem.attr("rm");
	remove(py::str(filename));
}
time_t PythonFilesystem::GetLastModifiedTime(FileHandle &handle) {
	// TODO: this value should be cached on the PythonFileHandle
	PythonGILWrapper gil;

	auto last_mod = filesystem.attr("modified")(handle.path);

	return py::int_(last_mod.attr("timestamp")());
}
void PythonFilesystem::FileSync(FileHandle &handle) {
	PythonGILWrapper gil;

	PythonFileHandle::GetHandle(handle)->attr("flush")();
}
} // namespace duckdb
