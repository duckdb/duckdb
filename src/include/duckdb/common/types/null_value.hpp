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

#include <cstring>

namespace duckdb {

//! This is no longer used in regular vectors, however, hash tables use this
//! value to store a NULL
template <class T> inline T NullValue() {
	return std::numeric_limits<T>::min();
}

constexpr const char str_nil[2] = {'\200', '\0'};

template <> inline const char *NullValue() {
	assert(str_nil[0] == '\200' && str_nil[1] == '\0');
	return str_nil;
}

template <> inline string_t NullValue() {
	return string_t(NullValue<const char *>());
}

template <> inline char *NullValue() {
	return (char *)NullValue<const char *>();
}

template <class T> inline bool IsNullValue(T value) {
	return value == NullValue<T>();
}

template <> inline bool IsNullValue(const char *value) {
	return *value == str_nil[0];
}

template <> inline bool IsNullValue(string_t value) {
	return value.GetData()[0] == str_nil[0];
}

template <> inline bool IsNullValue(char *value) {
	return IsNullValue<const char *>(value);
}

//! Compares a specific memory region against the types NULL value
bool IsNullValue(data_ptr_t ptr, TypeId type);

//! Writes NullValue<T> value of a specific type to a memory address
void SetNullValue(data_ptr_t ptr, TypeId type);

} // namespace duckdb
