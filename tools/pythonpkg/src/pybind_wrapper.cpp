#include "duckdb_python/pybind_wrapper.hpp"

namespace pybind11 {

bool gil_check() {
	return (bool)PyGILState_Check();
}

void gil_assert() {
	D_ASSERT(gil_check());
}

} // namespace pybind11
