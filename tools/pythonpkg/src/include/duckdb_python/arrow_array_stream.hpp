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
	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table) : arrow_object(arrow_table) {};

	//! Produces an Arrow Scanner, should be only called once when initializing Scan States
	static unique_ptr<ArrowArrayStreamWrapper>
	Produce(uintptr_t factory, std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	        TableFilterCollection *filters = nullptr);

	//! Get the schema of the arrow object
	static void GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema);

	//! Arrow Object (i.e., Scanner, Record Batch Reader, Table, Dataset)
	PyObject *arrow_object;

private:
	//! We transform a TableFilterCollection to an Arrow Expression Object
	static py::object TransformFilter(TableFilterCollection &filters, std::unordered_map<idx_t, string> &columns);

	static py::object ProduceScanner(py::object &arrow_scanner, py::handle &arrow_obj_handle,
	                                 std::pair<std::unordered_map<idx_t, string>, std::vector<string>> &project_columns,
	                                 TableFilterCollection *filters);
};
} // namespace duckdb