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
	explicit PythonTableArrowArrayStreamFactory(const py::object &arrow_table) : arrow_table(arrow_table) {};
	static ArrowArrayStream *Produce(uintptr_t factory);
	py::object arrow_table;
	ArrowArrayStream *stream = nullptr;
};
class PythonTableArrowArrayStream {
public:
	explicit PythonTableArrowArrayStream(const py::object &arrow_table);
	static void InitializeFunctionPointers(ArrowArrayStream *stream);
	ArrowArrayStream stream;

private:
	static int MyStreamGetSchema(ArrowArrayStream *stream, struct ArrowSchema *out);
	static int MyStreamGetNext(ArrowArrayStream *stream, struct ArrowArray *out);
	static void MyStreamRelease(ArrowArrayStream *stream);
	static const char *MyStreamGetLastError(ArrowArrayStream *stream);

	std::string last_error;
	py::object arrow_table;
	py::list batches;
	idx_t batch_idx = 0;
};
} // namespace duckdb