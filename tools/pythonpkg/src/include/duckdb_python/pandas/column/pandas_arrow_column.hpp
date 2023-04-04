#pragma once

#include "duckdb_python/pandas/pandas_column.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

class PandasArrowColumn : public PandasColumn {
public:
	PandasArrowColumn(py::object array) : PandasColumn(PandasColumnBackend::ARROW), array(std::move(array)) {
	}

public:
	py::object array;
};

} // namespace duckdb
