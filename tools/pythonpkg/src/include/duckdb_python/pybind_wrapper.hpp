//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

namespace duckdb {
#ifdef __GNUG__
#define PYBIND11_NAMESPACE pybind11 __attribute__((visibility("hidden")))
#else
#define PYBIND11_NAMESPACE pybind11
#endif
namespace py = pybind11;

} // namespace duckdb
