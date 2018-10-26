
#include "common/operator/cast_operators.hpp"
#include "common/types/date.hpp"

#include <cstdlib>

using namespace duckdb;
using namespace std;

namespace operators {

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <> int8_t Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int8_t>(value))
		return (int8_t)value;
	throw std::out_of_range("Cannot convert to TINYINT");
}

template <> int16_t Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int16_t>(value))
		return (int16_t)value;
	throw std::out_of_range("Cannot convert to SMALLINT");
}

template <> int Cast::Operation(const char *left) {
	return stoi(left, NULL, 10);
}

template <> int64_t Cast::Operation(const char *left) {
	return stoll(left, NULL, 10);
}

template <> uint64_t Cast::Operation(const char *left) {
	return stoull(left, NULL, 10);
}

template <> double Cast::Operation(const char *left) {
	return stod(left, NULL);
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <> std::string Cast::Operation(int8_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(int16_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(int left) {
	return to_string(left);
}

template <> std::string Cast::Operation(int64_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(uint64_t left) {
	return to_string(left);
}

template <> std::string Cast::Operation(double left) {
	return to_string(left);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <> std::string CastFromDate::Operation(duckdb::date_t left) {
	return Date::ToString(left);
}

template <> int32_t CastFromDate::Operation(date_t left) {
	return (int32_t)left;
}

template <> int64_t CastFromDate::Operation(date_t left) {
	return (int64_t)left;
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <> date_t CastToDate::Operation(const char *left) {
	return Date::FromString(left);
}

template <> date_t CastToDate::Operation(int32_t left) {
	return (date_t)left;
}

template <> date_t CastToDate::Operation(int64_t left) {
	return (date_t)left;
}

} // namespace operators
