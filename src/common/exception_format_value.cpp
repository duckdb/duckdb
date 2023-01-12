#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "fmt/format.h"
#include "fmt/printf.h"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

ExceptionFormatValue::ExceptionFormatValue(double dbl_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE), dbl_val(dbl_val) {
}
ExceptionFormatValue::ExceptionFormatValue(int64_t int_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER), int_val(int_val) {
}
ExceptionFormatValue::ExceptionFormatValue(hugeint_t huge_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(Hugeint::ToString(huge_val)) {
}
ExceptionFormatValue::ExceptionFormatValue(string str_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(std::move(str_val)) {
}

template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value) {
	return ExceptionFormatValue(TypeIdToString(value));
}
template <>
ExceptionFormatValue
ExceptionFormatValue::CreateFormatValue(LogicalType value) { // NOLINT: templating requires us to copy value here
	return ExceptionFormatValue(value.ToString());
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value) {
	return ExceptionFormatValue(double(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value) {
	return ExceptionFormatValue(double(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value) {
	return ExceptionFormatValue(std::move(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value) {
	return ExceptionFormatValue(string(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value) {
	return ExceptionFormatValue(string(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(hugeint_t value) {
	return ExceptionFormatValue(value);
}

string ExceptionFormatValue::Format(const string &msg, vector<ExceptionFormatValue> &values) {
	std::vector<duckdb_fmt::basic_format_arg<duckdb_fmt::printf_context>> format_args;
	for (auto &val : values) {
		switch (val.type) {
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.dbl_val));
			break;
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.int_val));
			break;
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.str_val));
			break;
		}
	}
	return duckdb_fmt::vsprintf(msg, duckdb_fmt::basic_format_args<duckdb_fmt::printf_context>(
	                                     format_args.data(), static_cast<int>(format_args.size())));
}

} // namespace duckdb
