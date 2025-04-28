#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pytype.hpp"

namespace duckdb {

class DuckDBPyWalReader {
public:
	DuckDBPyWalReader() = delete;

public:
	static void Initialize(py::module_ &m);
};

} // namespace duckdb
