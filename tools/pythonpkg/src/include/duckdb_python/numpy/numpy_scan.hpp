#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

struct PandasColumnBindData;

struct NumpyScan {
	static void Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out);
	static void ScanObjectColumn(PyObject **col, idx_t stride, idx_t count, idx_t offset, Vector &out);
};

} // namespace duckdb
