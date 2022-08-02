#include "duckdb_python/pybind_wrapper.hpp"

namespace py = pybind11;

namespace duckdb {

void ThrowHydratedError(const std::string &s);
void RegisterExceptions(const py::module &m);

} // namespace duckdb
