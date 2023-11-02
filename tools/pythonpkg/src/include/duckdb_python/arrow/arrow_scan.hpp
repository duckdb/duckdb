//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/arrow_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

struct PandasColumnBindData;

struct ArrowScan {
	static void Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out);
};

} // namespace duckdb
