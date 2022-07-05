#include "duckdb_python/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {
namespace python {

void RegisterExceptions(const py::module m);

} // namespace python
} // namespace duckdb
