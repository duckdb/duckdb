//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/vector_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb_python/pybind11/python_object_container.hpp"
#include "duckdb_python/pandas/pandas_analyzer.hpp"

namespace duckdb {

struct PandasColumnBindData {
	PandasType pandas_type;
	py::array numpy_col;
	idx_t numpy_stride;
	unique_ptr<RegisteredArray> mask;
	// Only for categorical types
	string internal_categorical_type;
	// When object types are cast we must hold their data somewhere
	PythonObjectContainer<py::str> object_str_val;
};

class VectorConversion {
public:
	static void NumpyToDuckDB(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset,
	                          Vector &out);

	static void BindPandas(const DBConfig &config, py::handle df, vector<PandasColumnBindData> &out,
	                       vector<LogicalType> &return_types, vector<string> &names);
};

} // namespace duckdb
