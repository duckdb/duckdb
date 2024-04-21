#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pybind11/registered_py_object.hpp"

namespace duckdb {

class PythonDependencies : public ExternalDependency {
public:
	static constexpr const ExternalDependenciesType TYPE = ExternalDependenciesType::PYTHON_DEPENDENCY;

public:
	explicit PythonDependencies(string name = string())
	    : ExternalDependency(ExternalDependenciesType::PYTHON_DEPENDENCY), name(name) {
	}
	~PythonDependencies() override;

public:
	void AddObject(const string &name, py::object object);
	void AddObject(const string &name, unique_ptr<RegisteredObject> &&object);
	bool HasName() const;
	const string &GetName() const;

public:
	//! Optional name for the dependency
	string name;
	//! The objects encompassed by this dependency
	case_insensitive_map_t<unique_ptr<RegisteredObject>> objects;
};

} // namespace duckdb
