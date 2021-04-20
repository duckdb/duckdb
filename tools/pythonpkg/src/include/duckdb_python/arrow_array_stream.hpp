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
    class PythonTableArrowArrayStream{
    public:
        explicit PythonTableArrowArrayStream(const py::object &arrow_table);
        static void InitializeFunctionPointers(ArrowArrayStream *stream);
        static ArrowArrayStream* PythonTableArrowArrayStreamFactory(uintptr_t arrow_table);
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