//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/null_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/windows_undefs.hpp"

#include <limits>
#include <cstring>
#include <cmath>

namespace duckdb {

//! Placeholder to insert in Vectors or to use for hashing NULLs
template <class T>
inline T NullValue() {
	return std::numeric_limits<T>::min();
}

constexpr const char str_nil[2] = {'\200', '\0'};

template <>
inline const char *NullValue() {
	D_ASSERT(str_nil[0] == '\200' && str_nil[1] == '\0');
	return str_nil;
}

template <>
inline string_t NullValue() {
	return string_t(NullValue<const char *>());
}

template <>
inline char *NullValue() {
	return (char *)NullValue<const char *>(); // NOLINT
}

template <>
inline string NullValue() {
	return string(NullValue<const char *>());
}

template <>
inline interval_t NullValue() {
	interval_t null_value;
	null_value.days = NullValue<int32_t>();
	null_value.months = NullValue<int32_t>();
	null_value.micros = NullValue<int64_t>();
	return null_value;
}

template <>
inline hugeint_t NullValue() {
	hugeint_t min;
	min.lower = 0;
	min.upper = std::numeric_limits<int64_t>::min();
	return min;
}

template <>
inline float NullValue() {
	return NAN;
}

template <>
inline double NullValue() {
	return NAN;
}

} // namespace duckdb
