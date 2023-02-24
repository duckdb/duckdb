#include "duckdb_python/pybind_wrapper.hpp"

namespace pybind11 {

// NOLINTNEXTLINE(readability-identifier-naming)
bool gil_check() {
	return (bool)PyGILState_Check();
}

// NOLINTNEXTLINE(readability-identifier-naming)
void gil_assert() {
	D_ASSERT(gil_check());
}

} // namespace pybind11
