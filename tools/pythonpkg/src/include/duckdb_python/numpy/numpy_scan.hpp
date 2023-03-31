#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

struct PandasColumnBindData;

struct Numpy {
	static void Scan(PandasColumnBindData &bind_data, py::array &numpy_col, idx_t count, idx_t offset, Vector &out);
};

} // namespace duckdb
