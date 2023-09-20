#include "duckdb_python/import_cache/python_import_cache.hpp"
#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PythonImportCacheItem (SUPER CLASS)
//===--------------------------------------------------------------------===//

py::handle PythonImportCacheItem::operator()(void) const {
	return object;
}

bool PythonImportCacheItem::LoadSucceeded() const {
	return load_succeeded;
}

bool PythonImportCacheItem::IsLoaded() const {
	auto type = (*this)();
	bool loaded = type.ptr() != nullptr;
	if (!loaded) {
		return false;
	}
	return true;
}

py::handle PythonImportCacheItem::AddCache(PythonImportCache &cache, py::object object) {
	return cache.AddCache(std::move(object));
}

void PythonImportCacheItem::LoadModule(const string &name, PythonImportCache &cache) {
	try {
		py::gil_assert();
		object = AddCache(cache, std::move(py::module::import(name.c_str())));
		load_succeeded = true;
	} catch (py::error_already_set &e) {
		if (IsRequired()) {
			throw InvalidInputException(
			    "Required module '%s' failed to import, due to the following Python exception:\n%s", name, e.what());
		}
		return;
	}
	LoadSubtypes(cache);
}

void PythonImportCacheItem::LoadAttribute(const string &name, PythonImportCache &cache, PythonImportCacheItem &source) {
	auto source_object = source();
	if (py::hasattr(source_object, name.c_str())) {
		object = AddCache(cache, std::move(source_object.attr(name.c_str())));
	} else {
		object = nullptr;
		return;
	}
	LoadSubtypes(cache);
}

//===--------------------------------------------------------------------===//
// PythonImportCache (CONTAINER)
//===--------------------------------------------------------------------===//

PythonImportCache::~PythonImportCache() {
	py::gil_scoped_acquire acquire;
	owned_objects.clear();
}

py::handle PythonImportCache::AddCache(py::object item) {
	auto object_ptr = item.ptr();
	owned_objects.push_back(std::move(item));
	return object_ptr;
}

} // namespace duckdb
