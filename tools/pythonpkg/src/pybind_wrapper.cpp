#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/exception.hpp"

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

} // namespace pybind11
