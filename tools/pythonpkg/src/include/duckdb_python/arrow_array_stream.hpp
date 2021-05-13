//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <atomic>
#include "duckdb/common/constants.hpp"
#include "duckdb/common/arrow_duckdb.hpp"
#include "pybind_wrapper.hpp"
namespace duckdb {
class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table) : arrow_table(arrow_table) {};
	static unique_ptr<ArrowArrayStreamDuck> Produce(uintptr_t factory);
	PyObject *arrow_table;
};

class PythonTableArrowArrayStream {
public:
	explicit PythonTableArrowArrayStream(PyObject *arrow_table, PythonTableArrowArrayStreamFactory *factory);

	unique_ptr<ArrowArrayStreamDuck> stream;
	PythonTableArrowArrayStreamFactory *factory;

private:
	static void InitializeFunctionPointers(ArrowArrayStream *stream);
	static int GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);
	static int GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);
	static void Release(struct ArrowArrayStream *stream);
	static const char *GetLastError(struct ArrowArrayStream *stream);

	std::string last_error;
	PyObject *arrow_table;
	py::list batches;
	std::atomic<idx_t> chunk_idx;
};
} // namespace duckdb