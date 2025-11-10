//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception_format_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/hugeint.hpp"

#include <vector>

namespace duckdb {

class String;

// Helper class to support custom overloading
// Escaping " and quoting the value with "
class SQLIdentifier {
public:
	explicit SQLIdentifier(const string &raw_string) : raw_string(raw_string) {
	}

public:
	string raw_string;
};

// Helper class to support custom overloading
// Escaping ' and quoting the value with '
class SQLString {
public:
	explicit SQLString(const string &raw_string) : raw_string(raw_string) {
	}

public:
	string raw_string;
};

enum class PhysicalType : uint8_t;
struct LogicalType;

enum class ExceptionFormatValueType : uint8_t {
	FORMAT_VALUE_TYPE_DOUBLE,
	FORMAT_VALUE_TYPE_INTEGER,
	FORMAT_VALUE_TYPE_STRING
};

struct ExceptionFormatValue {
	DUCKDB_API ExceptionFormatValue(double dbl_val);        // NOLINT
	DUCKDB_API ExceptionFormatValue(int64_t int_val);       // NOLINT
	DUCKDB_API ExceptionFormatValue(idx_t uint_val);        // NOLINT
	DUCKDB_API ExceptionFormatValue(string str_val);        // NOLINT
	DUCKDB_API ExceptionFormatValue(const String &str_val); // NOLINT
	DUCKDB_API ExceptionFormatValue(hugeint_t hg_val);      // NOLINT
	DUCKDB_API ExceptionFormatValue(uhugeint_t uhg_val);    // NOLINT

	ExceptionFormatValueType type;

	double dbl_val = 0;
	hugeint_t int_val = 0;
	string str_val;

public:
	template <class T>
	static ExceptionFormatValue CreateFormatValue(const T &value) {
		return int64_t(value);
	}
	static string Format(const string &msg, std::vector<ExceptionFormatValue> &values);
};

template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const PhysicalType &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const SQLString &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const SQLIdentifier &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const LogicalType &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const float &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const double &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const string &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const String &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *const &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *const &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const idx_t &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const hugeint_t &value);
template <>
DUCKDB_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const uhugeint_t &value);

} // namespace duckdb
