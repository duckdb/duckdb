#include "duckdb_python/python_dependency.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

PythonDependencyItem::PythonDependencyItem(unique_ptr<RegisteredObject> &&object) : object(std::move(object)) {
}

PythonDependencyItem::~PythonDependencyItem() { // NOLINT - cannot throw in exception
	py::gil_scoped_acquire gil;
	object.reset();
}

shared_ptr<DependencyItem> PythonDependencyItem::Create(py::object object) {
	auto registered_object = make_uniq<RegisteredObject>(std::move(object));
	return make_shared_ptr<PythonDependencyItem>(std::move(registered_object));
}

shared_ptr<DependencyItem> PythonDependencyItem::Create(unique_ptr<RegisteredObject> &&object) {
	return make_shared_ptr<PythonDependencyItem>(std::move(object));
}

} // namespace duckdb
