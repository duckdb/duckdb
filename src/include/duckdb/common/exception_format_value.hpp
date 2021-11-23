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
	DUCKDB_API ExceptionFormatValue(double dbl_val) // NOLINT
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE), dbl_val(dbl_val) {
	}
	DUCKDB_API ExceptionFormatValue(int64_t int_val) // NOLINT
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER), int_val(int_val) {
	}
	DUCKDB_API ExceptionFormatValue(string str_val) // NOLINT
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(move(str_val)) {
	}

	ExceptionFormatValueType type;

	double dbl_val;
	int64_t int_val;
	string str_val;

public:
	template <class T>
	static ExceptionFormatValue CreateFormatValue(T value) {
		return int64_t(value);
	}
	static string Format(const string &msg, vector<ExceptionFormatValue> &values);
};

template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value);

} // namespace duckdb
