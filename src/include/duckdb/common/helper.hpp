//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#include <algorithm>
#include <limits>
#include <sstream>

#ifdef _MSC_VER
#define suint64_t int64_t
#endif

namespace duckdb {
#if !defined(_MSC_VER) && (__cplusplus < 201402L)
template <typename T, typename... Args> unique_ptr<T> make_unique(Args &&... args) {
	return unique_ptr<T>(new T(std::forward<Args>(args)...));
}
#else // Visual Studio has make_unique
using std::make_unique;
#endif
template <typename S, typename T, typename... Args> unique_ptr<S> make_unique_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}

template <class T> inline bool in_bounds(int64_t value) {
	return value >= std::numeric_limits<T>::min() && value <= std::numeric_limits<T>::max();
}

template <typename T, typename S> unique_ptr<S> unique_ptr_cast(unique_ptr<T> src) {
	return unique_ptr<S>(static_cast<S *>(src.release()));
}

} // namespace duckdb
