#include "duckdb/common/assert.hpp"
#include "include/duckdb_python/arrow_array_stream.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

unique_ptr<ArrowArrayStreamWrapper> PythonTableArrowArrayStreamFactory::Produce(uintptr_t factory_ptr) {
	py::gil_scoped_acquire acquire;
	PythonTableArrowArrayStreamFactory *factory = (PythonTableArrowArrayStreamFactory *)factory_ptr;
	if (!factory->arrow_table) {
		return nullptr;
	}
	py::handle table(factory->arrow_table);
	py::object scanner;
	py::object arrow_scanner = py::module_::import("pyarrow.dataset").attr("Scanner").attr("from_dataset");
	auto py_object_type = string(py::str(table.get_type().attr("__name__")));

	if (py_object_type == "Table") {
		auto arrow_dataset = py::module_::import("pyarrow.dataset").attr("dataset");
		auto dataset = arrow_dataset(table);
		scanner = arrow_scanner(dataset);
	} else {
		scanner = arrow_scanner(table);
	}
	auto record_batches = scanner.attr("to_reader")();
	auto res = make_unique<ArrowArrayStreamWrapper>();
	auto export_to_c = record_batches.attr("_export_to_c");
	export_to_c((uint64_t)&res->arrow_array_stream);
	return res;
}

} // namespace duckdb