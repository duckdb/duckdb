//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas_dataframe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

class DataFrame : public py::object {
public:
	DataFrame(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object);
};

class PolarsDataFrame : public py::object {
public:
	PolarsDataFrame(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool IsDataFrame(const py::handle &object);
	static bool IsLazyFrame(const py::handle &object);
	static bool check_(const py::handle &object);
};
} // namespace duckdb
