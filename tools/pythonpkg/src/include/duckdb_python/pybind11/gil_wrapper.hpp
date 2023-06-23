#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

struct PythonGILWrapper {
	py::gil_scoped_acquire acquire;
};

} // namespace duckdb
