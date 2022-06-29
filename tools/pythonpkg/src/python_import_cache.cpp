#include "duckdb_python/python_import_cache.hpp"

namespace duckdb {

//! Cache Item

py::handle PythonImportCacheItem::operator()(void) {
	if (!object) {
		object = LoadObject();
	}
	return object;
}

//! Only require the cache item to interact directly with the cache
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

//! Cache

PythonImportCache::~PythonImportCache() {
	py::gil_scoped_acquire acquire;
	owned_objects.clear();
}

PyObject *PythonImportCache::AddCache(py::object item) {
	owned_objects.push_back(move(item));
	return owned_objects.back().ptr();
}

} // namespace duckdb
