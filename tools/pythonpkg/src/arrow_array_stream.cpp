#include "duckdb/common/assert.hpp"
#include "include/duckdb_python/arrow_array_stream.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/planner/table_filter.hpp"
namespace duckdb {
using namespace py::literals; // to bring in the `_a` literal


unique_ptr<ArrowArrayStreamWrapper> PythonTableArrowArrayStreamFactory::Produce(uintptr_t factory_ptr,
                                                                                std::vector<string> *project_columns,
                                                                                TableFilterCollection *filters) {
	py::gil_scoped_acquire acquire;
	PythonTableArrowArrayStreamFactory *factory = (PythonTableArrowArrayStreamFactory *)factory_ptr;
	if (!factory->arrow_table) {
		return nullptr;
	}
	py::handle table(factory->arrow_table);
	py::object scanner;
	py::object arrow_scanner = py::module_::import("pyarrow.dataset").attr("Scanner").attr("from_dataset");
	auto py_object_type = string(py::str(table.get_type().attr("__name__")));
	bool projection_pushdown = project_columns;

	if (projection_pushdown) {
		projection_pushdown = !project_columns->empty();
	}

	if (py_object_type == "Table") {
		auto arrow_dataset = py::module_::import("pyarrow.dataset").attr("dataset");
		auto dataset = arrow_dataset(table);
		if (projection_pushdown) {
			py::list projection_list = py::cast(*project_columns);
			scanner = arrow_scanner(dataset, "columns"_a = projection_list);
		} else {
			scanner = arrow_scanner(dataset);
		}
	} else {
		if (projection_pushdown) {
			py::list projection_list = py::cast(*project_columns);
			scanner = arrow_scanner(table, "columns"_a = projection_list);
		} else {
			scanner = arrow_scanner(table);
		}
	}
	auto record_batches = scanner.attr("to_reader")();
	auto res = make_unique<ArrowArrayStreamWrapper>();
	auto export_to_c = record_batches.attr("_export_to_c");
	export_to_c((uint64_t)&res->arrow_array_stream);
	return res;
}

py::object PythonTableArrowArrayStreamFactory::TransformFilter(TableFilterCollection &filters){
	filters.table_filters->filters;

}

} // namespace duckdb