#include "duckdb_python/python_import_cache.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PythonImportCacheItem (SUPER CLASS)
//===--------------------------------------------------------------------===//

py::handle PythonImportCacheItem::operator()(void) const {
	return object;
}

PyObject *PythonImportCacheItem::AddCache(PythonImportCache &cache, py::object object) {
	return cache.AddCache(move(object));
}

void PythonImportCacheItem::LoadModule(const string &name, PythonImportCache &cache) {
	object = AddCache(cache, move(py::module::import(name.c_str())));
	LoadSubtypes(cache);
}
void PythonImportCacheItem::LoadAttribute(const string &name, PythonImportCache &cache, PythonImportCacheItem &source) {
	auto source_object = source();
	object = AddCache(cache, move(source_object.attr(name.c_str())));
	LoadSubtypes(cache);
}

//===--------------------------------------------------------------------===//
// PythonImportCache (CONTAINER)
//===--------------------------------------------------------------------===//

PythonImportCache::~PythonImportCache() {
	py::gil_scoped_acquire acquire;
	owned_objects.clear();
}

PyObject *PythonImportCache::AddCache(py::object item) {
	auto object_ptr = item.ptr();
	owned_objects.push_back(move(item));
	return object_ptr;
}

} // namespace duckdb
