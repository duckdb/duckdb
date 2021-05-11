//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/common/constants.hpp"
#include "duckdb/common/arrow.hpp"
#include "pybind_wrapper.hpp"
namespace duckdb {
class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(py::handle arrow_table) : arrow_table(arrow_table) {};
	static unique_ptr<ArrowArrayStream> Produce(uintptr_t factory);
	py::handle arrow_table;
};

class PythonTableArrowArrayStream {
public:
	explicit PythonTableArrowArrayStream(PyObject *arrow_table, PythonTableArrowArrayStreamFactory *factory);
	static void InitializeFunctionPointers(ArrowArrayStream *stream);
	unique_ptr<ArrowArrayStream> stream;
	PythonTableArrowArrayStreamFactory *factory;

private:
	static int GetSchema(ArrowArrayStream *stream, struct ArrowSchema *out);
	static int GetNext(ArrowArrayStream *stream, struct ArrowArray *out, int chunk_idx);
	static void Release(ArrowArrayStream *stream);
	static const char *GetLastError(ArrowArrayStream *stream);

	std::string last_error;
	py::handle arrow_table;
	py::list batches;
};
} // namespace duckdb