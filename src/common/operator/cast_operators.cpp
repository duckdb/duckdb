#include "common/operator/cast_operators.hpp"

#include "common/exception.hpp"
#include "common/limits.hpp"
#include "common/types/date.hpp"
#include "common/types/timestamp.hpp"

#include <cstdlib>

using namespace duckdb;
using namespace std;

namespace duckdb {

template <class SRC, class DST> static DST cast_with_overflow_check(SRC value) {
	if (value < MinimumValue<DST>() || value > MaximumValue<DST>()) {
		throw ValueOutOfRangeException((int64_t)value, GetTypeId<SRC>(), GetTypeId<DST>());
	}
	return (DST)value;
}

template <class SRC> static uint64_t cast_to_uint64_overflow_check(SRC value) {
	if (value < 0) {
		throw ValueOutOfRangeException((int64_t)value, GetTypeId<SRC>(), TypeId::POINTER);
	}
	return (uint64_t)value;
}

template <class DST> static DST cast_from_uint64_overflow_check(uint64_t value) {
	if (value > (uint64_t)MaximumValue<DST>()) {
		throw ValueOutOfRangeException((int64_t)value, TypeId::POINTER, GetTypeId<DST>());
	}
	return (DST)value;
}

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <> int8_t Cast::Operation(int16_t left) {
	return cast_with_overflow_check<int16_t, int8_t>(left);
}
template <> int8_t Cast::Operation(int32_t left) {
	return cast_with_overflow_check<int32_t, int8_t>(left);
}
template <> int8_t Cast::Operation(int64_t left) {
	return cast_with_overflow_check<int64_t, int8_t>(left);
}
template <> int8_t Cast::Operation(uint64_t left) {
	return cast_from_uint64_overflow_check<int8_t>(left);
}
template <> int8_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int8_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> int16_t Cast::Operation(int32_t left) {
	return cast_with_overflow_check<int32_t, int16_t>(left);
}
template <> int16_t Cast::Operation(int64_t left) {
	return cast_with_overflow_check<int64_t, int16_t>(left);
}
template <> int16_t Cast::Operation(uint64_t left) {
	return cast_from_uint64_overflow_check<int16_t>(left);
}
template <> int16_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int16_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> int32_t Cast::Operation(int64_t left) {
	return cast_with_overflow_check<int64_t, int32_t>(left);
}
template <> int32_t Cast::Operation(uint64_t left) {
	return (int32_t)cast_from_uint64_overflow_check<uint64_t>(left);
}
template <> int32_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int32_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> int64_t Cast::Operation(uint64_t left) {
	return cast_from_uint64_overflow_check<uint64_t>(left);
}
template <> int64_t Cast::Operation(double left) {
	return cast_with_overflow_check<double, int64_t>(left);
}
//===--------------------------------------------------------------------===//
// Numeric -> uint64_t casts
//===--------------------------------------------------------------------===//
template <> uint64_t Cast::Operation(int8_t left) {
	return cast_to_uint64_overflow_check<int8_t>(left);
}
template <> uint64_t Cast::Operation(int16_t left) {
	return cast_to_uint64_overflow_check<int16_t>(left);
}
template <> uint64_t Cast::Operation(int32_t left) {
	return cast_to_uint64_overflow_check<int32_t>(left);
}
template <> uint64_t Cast::Operation(int64_t left) {
	return cast_to_uint64_overflow_check<int64_t>(left);
}
template <> uint64_t Cast::Operation(double left) {
	return cast_to_uint64_overflow_check<double>(left);
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//

template <> bool Cast::Operation(const char *left) {
	if (left[0] == 't' || left[0] == 'T') {
		return true;
	} else if (left[0] == 'f' || left[0] == 'F') {
		return false;
	} else {
		throw ConversionException("Could not convert string '%s' to boolean", left);
	}
}

template <> int8_t Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int8_t>(value))
		return (int8_t)value;
	throw ValueOutOfRangeException(value, TypeId::VARCHAR, TypeId::TINYINT);
}

template <> int16_t Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int16_t>(value))
		return (int16_t)value;
	throw ValueOutOfRangeException(value, TypeId::VARCHAR, TypeId::SMALLINT);
}

template <> int Cast::Operation(const char *left) {
	int64_t value = Cast::Operation<const char *, int64_t>(left);
	if (in_bounds<int32_t>(value))
		return (int32_t)value;
	throw ValueOutOfRangeException(value, TypeId::VARCHAR, TypeId::INTEGER);
}

template <> int64_t Cast::Operation(const char *left) {
	try {
		return stoll(left, NULL, 10);
	} catch (...) {
		throw ConversionException("Could not convert string '%s' to numeric", left);
	}
}

template <> uint64_t Cast::Operation(const char *left) {
	try {
		return stoull(left, NULL, 10);
	} catch (...) {
		throw ConversionException("Could not convert string '%s' to numeric", left);
	}
}

template <> float Cast::Operation(const char *left) {
	try {
		return (float)stod(left, NULL);
	} catch (...) {
		throw ConversionException("Could not convert string '%s' to numeric", left);
	}
}

template <> double Cast::Operation(const char *left) {
	try {
		return stod(left, NULL);
	} catch (...) {
		throw ConversionException("Could not convert string '%s' to numeric", left);
	}
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <> string Cast::Operation(bool left) {
	if (left) {
		return "true";
	} else {
		return "false";
	}
}

template <> string Cast::Operation(int8_t left) {
	return to_string(left);
}

template <> string Cast::Operation(int16_t left) {
	return to_string(left);
}

template <> string Cast::Operation(int left) {
	return to_string(left);
}

template <> string Cast::Operation(int64_t left) {
	return to_string(left);
}

template <> string Cast::Operation(uint64_t left) {
	return to_string(left);
}

template <> string Cast::Operation(float left) {
	return to_string(left);
}

template <> string Cast::Operation(double left) {
	return to_string(left);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <> string CastFromDate::Operation(date_t left) {
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

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//

template <> string CastFromTimestamp::Operation(timestamp_t left) {
	return Timestamp::ToString(left);
}

template <> int64_t CastFromTimestamp::Operation(timestamp_t left) {
	return (int64_t)left;
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <> timestamp_t CastToTimestamp::Operation(const char *left) {
	return Timestamp::FromString(left);
}

template <> timestamp_t CastToTimestamp::Operation(int64_t left) {
	return (timestamp_t)left;
}

} // namespace duckdb
