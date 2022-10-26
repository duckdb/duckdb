//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/arrow_array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "pybind_wrapper.hpp"

#include <string>
#include <vector>

namespace duckdb {

namespace pyarrow {

class RecordBatchReader : public py::object {
public:
	RecordBatchReader(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return !py::none().is(object);
	}
};
class Table : public py::object {
public:
	Table(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return !py::none().is(object);
	}
};
} // namespace pyarrow

enum class PyArrowObjectType { Invalid, Table, RecordBatchReader, Scanner, InMemoryDataset, FileSystemDataset };

PyArrowObjectType GetArrowType(const py::handle &obj);

class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table, ClientConfig &config)
	    : arrow_object(arrow_table), config(config) {};

	//! Produces an Arrow Scanner, should be only called once when initializing Scan States
	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory, ArrowStreamParameters &parameters);

	//! Get the schema of the arrow object
	static void GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema);

	//! Arrow Object (i.e., Scanner, Record Batch Reader, Table, Dataset)
	PyObject *arrow_object;

	ClientConfig &config;

private:
	//! We transform a TableFilterSet to an Arrow Expression Object
	static py::object TransformFilter(TableFilterSet &filters, std::unordered_map<idx_t, string> &columns,
	                                  ClientConfig &config);

	static py::object ProduceScanner(py::object &arrow_scanner, py::handle &arrow_obj_handle,
	                                 ArrowStreamParameters &parameters, ClientConfig &config);
};
} // namespace duckdb

namespace pybind11 {
namespace detail {
template <>
struct handle_type_name<duckdb::pyarrow::RecordBatchReader> {
	static constexpr auto name = _("pyarrow.lib.RecordBatchReader");
};
template <>
struct handle_type_name<duckdb::pyarrow::Table> {
	static constexpr auto name = _("pyarrow.lib.Table");
};
} // namespace detail
} // namespace pybind11
