#include "duckdb/common/operator/convert_to_string.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

template <class T>
string StandardStringCast(T input) {
	Vector v(LogicalType::VARCHAR);
	return StringCast::Operation(input, v).GetString();
}

template <>
string ConvertToString::Operation(bool input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int8_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int16_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int32_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int64_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint8_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint16_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint32_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint64_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(hugeint_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uhugeint_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(float input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(double input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(interval_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(date_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(dtime_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(timestamp_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(string_t input) {
	return input.GetString();
}

} // namespace duckdb
