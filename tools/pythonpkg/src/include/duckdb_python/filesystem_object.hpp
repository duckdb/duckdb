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
	explicit FileSystemObject(py::object fs, Value filenames_p)
	    : RegisteredObject(std::move(fs)), filenames(std::move(filenames_p)) {
		D_ASSERT(filenames.type().id() == LogicalTypeId::LIST);
	}
	virtual ~FileSystemObject() {
		py::gil_scoped_acquire acquire;
		// Assert that the 'obj' is a filesystem
		D_ASSERT(
		    py::isinstance(obj, DuckDBPyConnection::ImportCache()->pyduckdb().filesystem.modified_memory_filesystem()));
		auto &files = ListValue::GetChildren(filenames);
		for (auto &file : files) {
			auto name = StringValue::Get(file);
			obj.attr("delete")(name);
		}
	}

	Value filenames;
};

} // namespace duckdb
