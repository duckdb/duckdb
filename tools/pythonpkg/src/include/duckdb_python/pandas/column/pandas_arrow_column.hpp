//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas/column/pandas_arrow_column.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once

#include "duckdb_python/pandas/pandas_column.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {
class ChunkedArray : public py::object {
public:
	explicit ChunkedArray(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return !py::none().is(object);
	}
};

class PandasArrowColumn : public PandasColumn {
public:
	explicit PandasArrowColumn(ChunkedArray array_p) : PandasColumn(PandasColumnBackend::ARROW), array(std::move(array_p)) {
	}

public:
	ChunkedArray array;
};

} // namespace duckdb
