#include "duckdb_python/python_import_cache.hpp"

namespace duckdb {

//! Cache Item

py::object& PythonImportCacheItem::operator () (void) {
	if (!object) {
		object = LoadObject();
	}
	return *object;
}

//! Only require the cache item to interact directly with the cache
py::object* PythonImportCacheItem::AddCache(py::object object) {
	return cache.AddCache(move(object));
}

py::object* PythonImportCacheItem::LoadObject() {
	switch (item_type) {
		case PythonImportCacheItemType::MODULE:
			return LoadModule();
		case PythonImportCacheItemType::TYPE:
			return LoadAttribute();
		default:
			throw std::runtime_error("Type not implemented for PythonImportCacheItemType");
	}
}

py::object* PythonImportCacheItem::LoadModule() {
	return AddCache(move(py::module::import(name.c_str())));
}
py::object* PythonImportCacheItem::LoadAttribute() {
	auto& source_object = source();
	return AddCache(move(source_object.attr(name.c_str())));
}

//! Cache

PythonImportCache::~PythonImportCache() {
	py::gil_scoped_acquire acquire;
	owned_objects.clear();
}

py::object* PythonImportCache::AddCache(py::object item) {
	owned_objects.push_back(move(item));
	auto& cached_item = owned_objects.back();
	return &cached_item;
}

} //namespace duckdb
