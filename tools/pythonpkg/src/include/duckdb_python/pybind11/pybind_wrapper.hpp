//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind11//pybind_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include "duckdb/common/vector.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include <memory>

PYBIND11_DECLARE_HOLDER_TYPE(T, duckdb::unique_ptr<T>)
PYBIND11_DECLARE_HOLDER_TYPE(T, duckdb::shared_ptr<T>)

namespace pybind11 {

namespace detail {

template <typename Type, bool SAFE>
struct type_caster<duckdb::vector<Type, SAFE>> : list_caster<duckdb::vector<Type, SAFE>, Type> {};
} // namespace detail

bool gil_check();
void gil_assert();
bool is_list_like(handle obj);
bool is_dict_like(handle obj);

std::string to_string(const object &obj);

} // namespace pybind11

namespace duckdb {
#ifdef __GNUG__
#define PYBIND11_NAMESPACE pybind11 __attribute__((visibility("hidden")))
#else
#define PYBIND11_NAMESPACE pybind11
#endif
namespace py {

// We include everything from pybind11
using namespace pybind11;

// But we have the option to override certain functions
template <typename T, detail::enable_if_t<std::is_base_of<object, T>::value, int> = 0>
bool isinstance(handle obj) {
	return T::check_(obj);
}

template <typename T, detail::enable_if_t<!std::is_base_of<object, T>::value, int> = 0>
bool isinstance(handle obj) {
	return detail::isinstance_generic(obj, typeid(T));
}

template <>
inline bool isinstance<handle>(handle) = delete;
template <>
inline bool isinstance<object>(handle obj) {
	return obj.ptr() != nullptr;
}

inline bool isinstance(handle obj, handle type) {
	if (type.ptr() == nullptr) {
		// The type was not imported, just return false
		return false;
	}
	const auto result = PyObject_IsInstance(obj.ptr(), type.ptr());
	if (result == -1) {
		throw error_already_set();
	}
	return result != 0;
}

template <class T>
bool try_cast(const handle &object, T &result) {
	try {
		result = cast<T>(object);
	} catch (pybind11::cast_error &) {
		return false;
	}
	return true;
}

} // namespace py

template <class T, typename... ARGS>
void DefineMethod(std::vector<const char *> aliases, T &mod, ARGS &&... args) {
	for (auto &alias : aliases) {
		mod.def(alias, args...);
	}
}

} // namespace duckdb
