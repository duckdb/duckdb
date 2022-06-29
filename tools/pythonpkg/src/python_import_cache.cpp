#include "duckdb_python/python_import_cache.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PythonImportCacheItem (SUPER CLASS)
//===--------------------------------------------------------------------===//

py::handle PythonImportCacheItem::operator()(void) {
	return object;
}

//! Only require the super class to interact directly with the cache
PyObject *PythonImportCacheItem::AddCache(py::object object) {
	return cache.AddCache(move(object));
}

PyObject *PythonImportCacheItem::LoadObject() {
	switch (item_type) {
	case PythonImportCacheItemType::MODULE:
		return LoadModule();
	case PythonImportCacheItemType::TYPE:
		return LoadAttribute();
	default:
		throw std::runtime_error("Type not implemented for PythonImportCacheItemType");
	}
}

PyObject *PythonImportCacheItem::LoadModule() {
	return AddCache(move(py::module::import(name.c_str())));
}
PyObject *PythonImportCacheItem::LoadAttribute() {
	auto source_object = source();
	return AddCache(move(source_object.attr(name.c_str())));
}

//===--------------------------------------------------------------------===//
// PythonImportCache (CONTAINER)
//===--------------------------------------------------------------------===//

PythonImportCache::~PythonImportCache() {
	py::gil_scoped_acquire acquire;
	owned_objects.clear();
}

PyObject *PythonImportCache::AddCache(py::object item) {
	auto registered_object = make_unique<RegisteredObject>(move(item));
	auto object_ptr = registered_object->obj.ptr();
	owned_objects.push_back(move(registered_object));
	return object_ptr;
}

} // namespace duckdb
