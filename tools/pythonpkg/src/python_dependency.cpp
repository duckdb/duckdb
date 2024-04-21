#include "duckdb_python/python_dependency.hpp"

namespace duckdb {

void PythonDependencies::AddObject(const string &name, py::object object) {
	auto registered_object = make_uniq<RegisteredObject>(std::move(object));
	AddObject(name, std::move(registered_object));
}

void PythonDependencies::AddObject(const string &name, unique_ptr<RegisteredObject> &&object) {
	objects.emplace(std::make_pair(name, std::move(object)));
}

PythonDependencies::~PythonDependencies() {
	py::gil_scoped_acquire gil;
	objects.clear();
}

bool PythonDependencies::HasName() const {
	return !name.empty();
}

const string &PythonDependencies::GetName() const {
	D_ASSERT(HasName());
	return name;
}

} // namespace duckdb
