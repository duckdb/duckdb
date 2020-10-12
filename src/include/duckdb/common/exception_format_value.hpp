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
	ExceptionFormatValue(double dbl_val) : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE), dbl_val(dbl_val) {
	}
	ExceptionFormatValue(int64_t int_val)
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER), int_val(int_val) {
	}
	ExceptionFormatValue(string str_val) : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(str_val) {
	}

	ExceptionFormatValueType type;

	double dbl_val;
	int64_t int_val;
	string str_val;

public:
	template <class T> static ExceptionFormatValue CreateFormatValue(T value) {
		return int64_t(value);
	}
	static string Format(string msg, vector<ExceptionFormatValue> &values);
};

template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value);
template <> ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value);

} // namespace duckdb
