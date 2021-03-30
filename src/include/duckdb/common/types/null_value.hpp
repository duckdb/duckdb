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
#include "duckdb/common/limits.hpp"
#include "duckdb/common/windows_undefs.hpp"

#include <limits>
#include <cstring>
#include <cmath>

namespace duckdb {

//! This is no longer used in regular vectors, however, hash tables use this
//! value to store a NULL
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
	return (char *)NullValue<const char *>();
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

template <class T>
inline bool IsNullValue(T value) {
	return value == NullValue<T>();
}

template <>
inline bool IsNullValue(const char *value) {
	return *value == str_nil[0];
}

template <>
inline bool IsNullValue(string_t value) {
	return value.GetDataUnsafe()[0] == str_nil[0];
}

template <>
inline bool IsNullValue(interval_t value) {
	return value.days == NullValue<int32_t>() && value.months == NullValue<int32_t>() &&
	       value.micros == NullValue<int64_t>();
}

template <>
inline bool IsNullValue(char *value) {
	return IsNullValue<const char *>(value);
}

template <>
inline bool IsNullValue(float value) {
	return std::isnan(value);
}

template <>
inline bool IsNullValue(double value) {
	return std::isnan(value);
}

//! Compares a specific memory region against the types NULL value
bool IsNullValue(data_ptr_t ptr, PhysicalType type);

//! Writes NullValue<T> value of a specific type to a memory address
void SetNullValue(data_ptr_t ptr, PhysicalType type);

} // namespace duckdb
