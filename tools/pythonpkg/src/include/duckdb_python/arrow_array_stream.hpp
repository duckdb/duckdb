//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/arrow_array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/arrow_wrapper.hpp"
#include "pybind_wrapper.hpp"
namespace duckdb {
class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table) : arrow_table(arrow_table) {};
	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory);
	PyObject *arrow_table;
};
} // namespace duckdb