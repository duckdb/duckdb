#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/pytype.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"

namespace duckdb {

class DuckDBPyTyping {
public:
	DuckDBPyTyping() = delete;

public:
	static void Initialize(py::module_ &m);
};

} // namespace duckdb
