#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb_python/python_instance_checker.hpp"
#include "datetime.h" // from Python

namespace duckdb {

Value TransformPythonValue(PythonInstanceChecker& instance_checker, py::handle ele);

} //namespace duckdb
