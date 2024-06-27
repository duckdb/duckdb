#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace pybind11 {

// NOLINTNEXTLINE(readability-identifier-naming)
bool gil_check() {
	return (bool)PyGILState_Check();
}

// NOLINTNEXTLINE(readability-identifier-naming)
void gil_assert() {
	if (!gil_check()) {
		throw duckdb::InternalException("The GIL should be held for this operation, but it's not!");
	}
}

// NOLINTNEXTLINE(readability-identifier-naming)
bool is_list_like(handle obj) {
	if (isinstance<str>(obj) || isinstance<bytes>(obj)) {
		return false;
	}
	if (is_dict_like(obj)) {
		return false;
	}
	auto &import_cache = *duckdb::DuckDBPyConnection::ImportCache();
	auto iterable = import_cache.collections.abc.Iterable();
	return isinstance(obj, iterable);
}

// NOLINTNEXTLINE(readability-identifier-naming)
bool is_dict_like(handle obj) {
	auto &import_cache = *duckdb::DuckDBPyConnection::ImportCache();
	auto mapping = import_cache.collections.abc.Mapping();
	return isinstance(obj, mapping);
}

} // namespace pybind11
