#pragma once

#include "duckdb_python/pandas/pandas_column.hpp"
#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/arrow/arrow_array_stream.hpp"

namespace duckdb {

class PandasArrowColumn : public PandasColumn {
public:
	PandasArrowColumn(py::object array_p, const ClientConfig &config) : PandasColumn(PandasColumnBackend::ARROW) {
		py::list single_column_list;
		single_column_list.append(array_p);
		py::list single_name_list;
		// We just need the name to create a table, but we don't care about it, the table only consists of this array
		single_name_list.append(py::str("temp"));

		table = py::module_::import("pyarrow").attr("lib").attr("Table").attr("from_arrays")(single_column_list, py::arg("names") = single_name_list);
		auto factory = make_unique<PythonTableArrowArrayStreamFactory>(table.ptr(), config);

		auto stream_factory_produce = PythonTableArrowArrayStreamFactory::Produce;
		arrow_scan = make_unique<ArrowScanFunctionData>(stream_factory_produce, factory.ptr());
	}

public:
	py::object table;
	unique_ptr<ArrowScanFunctionData> arrow_scan;
	unique_ptr<LocalTableFunctionState> local_state;
	unique_ptr<GlobalTableFunctionState> global_state;
};

} // namespace duckdb
