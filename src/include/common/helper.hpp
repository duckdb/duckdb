//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

#include <limits>
#include <sstream>
#include <algorithm>

#ifdef _MSC_VER
#define ssize_t int64_t
#endif

namespace duckdb {
#ifndef _MSC_VER
template <typename T, typename... Args> unique_ptr<T> make_unique(Args &&... args) {
	return unique_ptr<T>(new T(std::forward<Args>(args)...));
}
#else // Visual Studio has make_unique
#include <memory>
#define make_unique std::make_unique
#endif
template <typename S, typename T, typename... Args> unique_ptr<S> make_unique_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}

template <class T> inline bool in_bounds(int64_t value) {
	return value >= std::numeric_limits<T>::min() && value <= std::numeric_limits<T>::max();
}

} // namespace duckdb
