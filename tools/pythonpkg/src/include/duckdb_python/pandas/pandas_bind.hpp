#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pybind11/python_object_container.hpp"
#include "duckdb_python/numpy/numpy_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb_python/pandas/pandas_column.hpp"

namespace duckdb {

struct RegisteredArray;

struct PandasColumnBindData {
	NumpyNullableType numpy_type;
	unique_ptr<PandasColumn> numpy_col;
	unique_ptr<RegisteredArray> mask;
	// Only for categorical types
	string internal_categorical_type;
	// When object types are cast we must hold their data somewhere
	PythonObjectContainer<py::str> object_str_val;
};

struct Pandas {
	static void Bind(const DBConfig &config, py::handle df, vector<PandasColumnBindData> &out,
	                 vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
