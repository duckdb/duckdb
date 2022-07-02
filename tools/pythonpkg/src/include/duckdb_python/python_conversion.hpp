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
#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"

#include "datetime.h" // from Python

namespace duckdb {

bool DictionaryHasMapFormat(const PyDictionary &dict);
Value TransformPythonValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN);

} // namespace duckdb
