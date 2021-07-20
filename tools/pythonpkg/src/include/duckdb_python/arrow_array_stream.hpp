//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/arrow_array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "pybind_wrapper.hpp"

#include "duckdb/function/table_function.hpp"
#include <string>
#include <vector>
namespace duckdb {
class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table) : arrow_table(arrow_table) {};
	static unique_ptr<ArrowArrayStreamWrapper>
	Produce(uintptr_t factory, std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	        TableFilterCollection *filters = nullptr);
	PyObject *arrow_table;

private:
	//! We transform a TableFilterCollection to an Arrow Expression Object
	static py::object TransformFilter(TableFilterCollection &filters, std::unordered_map<idx_t, string> &columns);
};
} // namespace duckdb