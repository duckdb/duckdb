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
#include "duckdb/common/unique_ptr.hpp"
#include <vector>

namespace py = pybind11;

PYBIND11_DECLARE_HOLDER_TYPE(T, duckdb::unique_ptr<T>);

namespace duckdb {
#ifdef __GNUG__
#define PYBIND11_NAMESPACE pybind11 __attribute__((visibility("hidden")))
#else
#define PYBIND11_NAMESPACE pybind11
#endif
namespace py = pybind11;

template <class T, typename... ARGS>
void DefineMethod(std::vector<const char *> aliases, T &mod, ARGS &&... args) {
	for (auto &alias : aliases) {
		mod.def(alias, args...);
	}
}

} // namespace duckdb
