#pragma once

#include "duckdb_python/pandas/pandas_column.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

class PandasNumpyColumn : public PandasColumn {
public:
	PandasNumpyColumn(py::array array_p) : PandasColumn(PandasColumnBackend::NUMPY), array(std::move(array_p)) {
		D_ASSERT(py::hasattr(array, "strides"));
		stride = array.attr("strides").attr("__getitem__")(0).cast<idx_t>();
	}

public:
	py::array array;
	idx_t stride;
};

} // namespace duckdb
