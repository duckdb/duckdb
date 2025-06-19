//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pyresult.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "datetime.h" // from Python

namespace duckdb {

enum class PythonObjectType {
	Other,
	None,
	Integer,
	Float,
	Bool,
	Decimal,
	Uuid,
	Datetime,
	Date,
	Time,
	Timedelta,
	String,
	ByteArray,
	MemoryView,
	Bytes,
	List,
	Tuple,
	Dict,
	NdArray,
	NdDatetime,
	Value
};

PythonObjectType GetPythonObjectType(py::handle &ele);

bool TryTransformPythonNumeric(Value &res, py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN);
bool DictionaryHasMapFormat(const PyDictionary &dict);
void TransformPythonObject(py::handle ele, Vector &vector, idx_t result_offset, bool nan_as_null = true);
Value TransformPythonValue(py::handle ele, const LogicalType &target_type = LogicalType::UNKNOWN,
                           bool nan_as_null = true);

} // namespace duckdb
