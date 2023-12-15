#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {

void RegisterExceptions(const py::module &m);

} // namespace duckdb
