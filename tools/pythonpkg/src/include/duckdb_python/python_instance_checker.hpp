//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/object_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct PythonTypeWrapper;

//! Interface that wraps a python module and provides methods to get a type from the module and caches the imported
//! types
struct PythonModuleWrapper {
public:
	PythonModuleWrapper(const string &name, py::handle module_) : name(name), module_(move(module_)), type_map() {
	}

public:
	PythonTypeWrapper &Get(const string &type);
	pair<unordered_map<string, unique_ptr<PythonTypeWrapper>>::iterator, bool> RegisterType(const string &type_name);

private:
	string name;
	py::handle module_;
	unordered_map<string, unique_ptr<PythonTypeWrapper>> type_map;
};

//! Might be overkill..
struct PythonTypeWrapper {
public:
	PythonTypeWrapper(const string &name, py::handle type) : name(name), type(move(type)) {
	}

public:
	bool InstanceOf(py::handle object);

private:
	string name;
	py::handle type;
};

//! Caches imported modules and types, provides an interface to easily check instance of an object
class PythonInstanceChecker {
public:
	PythonInstanceChecker() : module_map() {
	}

public:
	template <typename T>
	bool IsInstanceOf(py::handle object) {
		return py::isinstance<T>(object);
	}
	bool IsInstanceOf(py::handle object, const string &type, const string &module);
	pair<unordered_map<string, unique_ptr<PythonModuleWrapper>>::iterator, bool>
	RegisterModule(const string &module_name);

private:
	PythonModuleWrapper &GetModule(const string &module_name);

private:
	unordered_map<string, unique_ptr<PythonModuleWrapper>> module_map;
};

} // namespace duckdb
