//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception_format_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class ExceptionFormatValueType : uint8_t {
	FORMAT_VALUE_TYPE_DOUBLE,
	FORMAT_VALUE_TYPE_INTEGER,
	FORMAT_VALUE_TYPE_STRING
};

struct ExceptionFormatValue {
	DUCKDB_API ExceptionFormatValue(double dbl_val);   // NOLINT
	DUCKDB_API ExceptionFormatValue(int64_t int_val);  // NOLINT
	DUCKDB_API ExceptionFormatValue(string str_val);   // NOLINT
	DUCKDB_API ExceptionFormatValue(hugeint_t hg_val); // NOLINT

	ExceptionFormatValueType type;

	double dbl_val = 0;
	int64_t int_val = 0;
	string str_val;

public:
	template <class T>
	static ExceptionFormatValue CreateFormatValue(T value) {
		return int64_t(value);
	}
	static string Format(const string &msg, vector<ExceptionFormatValue> &values);
};

template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(hugeint_t value);

} // namespace duckdb
