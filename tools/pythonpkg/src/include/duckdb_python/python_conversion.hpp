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
#include "duckdb/common/types.hpp"

#include "datetime.h" // from Python

namespace duckdb {

struct PyDictionary {
public:
	PyDictionary(py::object dict);
	py::object keys;
	py::object values;
	idx_t len;

private:
	py::object dict;
};

bool DictionaryHasMapFormat(const PyDictionary &dict);
Value TransformPythonValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN);

} // namespace duckdb
