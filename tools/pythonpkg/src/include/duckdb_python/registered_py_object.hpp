//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/registered_py_object.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

class RegisteredObject {
public:
	explicit RegisteredObject(py::object obj_p) : obj(move(obj_p)) {
	}
	virtual ~RegisteredObject() {
		obj = py::none();
	}

	py::object obj;
};

} // namespace duckdb
