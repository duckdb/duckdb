//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "array_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

#include "datetime.h" // from Python

namespace duckdb {

bool DictionaryHasMapFormat(py::handle dict_values, idx_t len);
Value TransformPythonValue(py::handle ele);

} // namespace duckdb
