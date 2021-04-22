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
namespace duckdb{
class PythonArrowFunctions {
public:
    PythonArrowFunctions() {
//        py::gil_scoped_acquire acquire;
        batch_export_to_c = py::module::import("pyarrow").attr("RecordBatch").attr("_export_to_c");
        D_ASSERT(batch_export_to_c);
    }
    py::handle batch_export_to_c;
};

    class PythonTableArrowArrayStream{
    public:
        explicit PythonTableArrowArrayStream(const py::object &arrow_table);
        static void InitializeFunctionPointers(ArrowArrayStream *stream);
        static ArrowArrayStream* PythonTableArrowArrayStreamFactory(uintptr_t arrow_table);
	    static PythonArrowFunctions functions;
    private:
        static int MyStreamGetSchema(ArrowArrayStream *stream, struct ArrowSchema *out);
        static int MyStreamGetNext(ArrowArrayStream *stream, struct ArrowArray *out);
        static void MyStreamRelease(ArrowArrayStream *stream);
        static const char *MyStreamGetLastError(ArrowArrayStream *stream);
        ArrowArrayStream stream;
        std::string last_error;
        py::object arrow_table;
        py::list batches;
        idx_t batch_idx = 0;

    };
}