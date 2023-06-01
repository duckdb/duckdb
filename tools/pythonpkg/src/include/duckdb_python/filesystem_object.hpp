//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/filesystem_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/registered_py_object.hpp"
#include "duckdb_python/pyfilesystem.hpp"

namespace duckdb {

class FileSystemObject : public RegisteredObject {
public:
	explicit FileSystemObject(py::object fs, const string &filename)
	    : RegisteredObject(std::move(fs)), filename(filename) {
	}
	virtual ~FileSystemObject() {
		py::gil_scoped_acquire acquire;
		// Assert that the 'obj' is a filesystem
		auto &import_cache = *DuckDBPyConnection::ImportCache();
		D_ASSERT(import_cache.pyduckdb().filesystem.modified_memory_filesystem.IsInstance(obj));
		obj.attr("delete")(filename);
	}

	string filename;
};

} // namespace duckdb
