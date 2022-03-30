#pragma once

#include "duckdb.hpp"

namespace duckdb {

typedef uint32_t variant_index_type;

template <typename T>
struct always_false : std::false_type {};

template <typename T>
Value Variant(T value) {
	static_assert(always_false<T>::value, "Cannot convert type to Variant");
	return Value();
}

template <>
Value DUCKDB_API Variant(bool value);
template <>
Value DUCKDB_API Variant(int8_t value);
template <>
Value DUCKDB_API Variant(uint8_t value);
template <>
Value DUCKDB_API Variant(int16_t value);
template <>
Value DUCKDB_API Variant(uint16_t value);
template <>
Value DUCKDB_API Variant(int32_t value);
template <>
Value DUCKDB_API Variant(uint32_t value);
template <>
Value DUCKDB_API Variant(int64_t value);
template <>
Value DUCKDB_API Variant(uint64_t value);
template <>
Value DUCKDB_API Variant(hugeint_t value);
template <>
Value DUCKDB_API Variant(float value);
template <>
Value DUCKDB_API Variant(double value);
template <>
Value DUCKDB_API Variant(date_t value);
template <>
Value DUCKDB_API Variant(dtime_t value);
template <>
Value DUCKDB_API Variant(timestamp_t value);
template <>
Value DUCKDB_API Variant(interval_t value);

Value DUCKDB_API Variant(const char *value);
Value DUCKDB_API Variant(const string &value);
Value DUCKDB_API Variant(const Value &value);

Value DUCKDB_API FromVariant(const Value &value);

} // namespace duckdb
