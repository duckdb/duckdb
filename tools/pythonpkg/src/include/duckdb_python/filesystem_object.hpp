//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/filesystem_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/pybind11/registered_py_object.hpp"
#include "duckdb_python/pyfilesystem.hpp"

namespace duckdb {

class FileSystemObject : public RegisteredObject {
public:
	explicit FileSystemObject(py::object fs, vector<string> filenames_p)
	    : RegisteredObject(std::move(fs)), filenames(std::move(filenames_p)) {
	}
	~FileSystemObject() override {
		py::gil_scoped_acquire acquire;
		// Assert that the 'obj' is a filesystem
		D_ASSERT(py::isinstance(obj, DuckDBPyConnection::ImportCache()->duckdb.filesystem.ModifiedMemoryFileSystem()));
		for (auto &file : filenames) {
			obj.attr("delete")(file);
		}
	}

	vector<string> filenames;
};

} // namespace duckdb
