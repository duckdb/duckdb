//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include <string.h>

#ifdef _MSC_VER
#define suint64_t int64_t
#endif

namespace duckdb {
#if !defined(_MSC_VER) && (__cplusplus < 201402L)
template <typename T, typename... Args>
unique_ptr<T> make_unique(Args &&... args) {
	return unique_ptr<T>(new T(std::forward<Args>(args)...));
}
#else // Visual Studio has make_unique
using std::make_unique;
#endif
template <typename S, typename T, typename... Args>
unique_ptr<S> make_unique_base(Args &&...args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}

template <typename T, typename S>
unique_ptr<S> unique_ptr_cast(unique_ptr<T> src) {
	return unique_ptr<S>(static_cast<S *>(src.release()));
}

template <typename T>
T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template <typename T>
T MinValue(T a, T b) {
	return a < b ? a : b;
}

template <typename T>
const T Load(const_data_ptr_t ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret));
	return ret;
}

template <typename T>
void Store(const T val, data_ptr_t ptr) {
	memcpy(ptr, (void *)&val, sizeof(val));
}

} // namespace duckdb
