#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

namespace pyarrow {

py::object ToArrowTable(const vector<LogicalType> &types, const vector<string> &names, const string &timezone_config,
                        std::function<py::list(const string &timezone_config)> batch_provider);

} // namespace pyarrow

} // namespace duckdb
