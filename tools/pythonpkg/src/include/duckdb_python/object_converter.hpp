//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/object_converter.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct PythonTypeWrapper;

//! Interface that wraps any python module
struct PythonModuleWrapper {
public:
	PythonModuleWrapper() {}
	virtual ~PythonModuleWrapper() {}
public:
	virtual PythonTypeWrapper& Get(const string& type) = 0;
	virtual void	RegisterType(const string& type_name) = 0;
private:
	unordered_map<string, unique_ptr<PythonTypeWrapper>>& type_map;
};

struct PythonTypeWrapper {
public:
	PythonTypeWrapper() {}
	virtual ~PythonTypeWrapper() {}
public:
	bool InstanceOf(py::handle object) const = 0;
};

//Checks if an object is of a specific type
class ObjectConverter {
public:
	ObjectConverter() {}
public:

private:
private:
	unordered_map<string, unique_ptr<PythonTypeWrapper>> type_map;
};

} //namespace duckdb
