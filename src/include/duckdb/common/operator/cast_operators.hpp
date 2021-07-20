//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/convert_to_string.hpp"

namespace duckdb {
class Vector;

struct TryCast {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported cast from %s to %s", GetTypeId<SRC>(), GetTypeId<DST>());
	}
};

template<class SRC, class DST>
static string CastException(SRC input) {
	return "Type " + TypeIdToString(GetTypeId<SRC>()) + " with value " +
								ConvertToString::Operation<SRC>(input) +
								" can't be cast to the destination type " +
								TypeIdToString(GetTypeId<DST>());
}

struct Cast {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		DST result;
		if (!TryCast::Operation(input, result)) {
			throw InvalidInputException(CastException<SRC, DST>(input));
		}
		return result;
	}
};

#define TRY_CAST_TEMPLATE(SOURCE_TYPE, TARGET_TYPE) \
template <> \
bool TryCast::Operation(SOURCE_TYPE input, TARGET_TYPE &result, bool strict);

//===--------------------------------------------------------------------===//
// bool casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(bool, bool)
TRY_CAST_TEMPLATE(bool, int8_t)
TRY_CAST_TEMPLATE(bool, int16_t)
TRY_CAST_TEMPLATE(bool, int32_t)
TRY_CAST_TEMPLATE(bool, int64_t)
TRY_CAST_TEMPLATE(bool, uint8_t)
TRY_CAST_TEMPLATE(bool, uint16_t)
TRY_CAST_TEMPLATE(bool, uint32_t)
TRY_CAST_TEMPLATE(bool, uint64_t)
TRY_CAST_TEMPLATE(bool, float)
TRY_CAST_TEMPLATE(bool, double)

//===--------------------------------------------------------------------===//
// int8_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(int8_t, bool)
TRY_CAST_TEMPLATE(int8_t, int8_t)
TRY_CAST_TEMPLATE(int8_t, int16_t)
TRY_CAST_TEMPLATE(int8_t, int32_t)
TRY_CAST_TEMPLATE(int8_t, int64_t)
TRY_CAST_TEMPLATE(int8_t, uint8_t)
TRY_CAST_TEMPLATE(int8_t, uint16_t)
TRY_CAST_TEMPLATE(int8_t, uint32_t)
TRY_CAST_TEMPLATE(int8_t, uint64_t)
TRY_CAST_TEMPLATE(int8_t, float)
TRY_CAST_TEMPLATE(int8_t, double)

//===--------------------------------------------------------------------===//
// int16_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(int16_t, bool)
TRY_CAST_TEMPLATE(int16_t, int8_t)
TRY_CAST_TEMPLATE(int16_t, int16_t)
TRY_CAST_TEMPLATE(int16_t, int32_t)
TRY_CAST_TEMPLATE(int16_t, int64_t)
TRY_CAST_TEMPLATE(int16_t, uint8_t)
TRY_CAST_TEMPLATE(int16_t, uint16_t)
TRY_CAST_TEMPLATE(int16_t, uint32_t)
TRY_CAST_TEMPLATE(int16_t, uint64_t)
TRY_CAST_TEMPLATE(int16_t, float)
TRY_CAST_TEMPLATE(int16_t, double)

//===--------------------------------------------------------------------===//
// int32_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(int32_t, bool)
TRY_CAST_TEMPLATE(int32_t, int8_t)
TRY_CAST_TEMPLATE(int32_t, int16_t)
TRY_CAST_TEMPLATE(int32_t, int32_t)
TRY_CAST_TEMPLATE(int32_t, int64_t)
TRY_CAST_TEMPLATE(int32_t, uint8_t)
TRY_CAST_TEMPLATE(int32_t, uint16_t)
TRY_CAST_TEMPLATE(int32_t, uint32_t)
TRY_CAST_TEMPLATE(int32_t, uint64_t)
TRY_CAST_TEMPLATE(int32_t, float)
TRY_CAST_TEMPLATE(int32_t, double)

//===--------------------------------------------------------------------===//
// int64_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(int64_t, bool)
TRY_CAST_TEMPLATE(int64_t, int8_t)
TRY_CAST_TEMPLATE(int64_t, int16_t)
TRY_CAST_TEMPLATE(int64_t, int32_t)
TRY_CAST_TEMPLATE(int64_t, int64_t)
TRY_CAST_TEMPLATE(int64_t, uint8_t)
TRY_CAST_TEMPLATE(int64_t, uint16_t)
TRY_CAST_TEMPLATE(int64_t, uint32_t)
TRY_CAST_TEMPLATE(int64_t, uint64_t)
TRY_CAST_TEMPLATE(int64_t, float)
TRY_CAST_TEMPLATE(int64_t, double)

//===--------------------------------------------------------------------===//
// uint8_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(uint8_t, bool)
TRY_CAST_TEMPLATE(uint8_t, int8_t)
TRY_CAST_TEMPLATE(uint8_t, int16_t)
TRY_CAST_TEMPLATE(uint8_t, int32_t)
TRY_CAST_TEMPLATE(uint8_t, int64_t)
TRY_CAST_TEMPLATE(uint8_t, uint8_t)
TRY_CAST_TEMPLATE(uint8_t, uint16_t)
TRY_CAST_TEMPLATE(uint8_t, uint32_t)
TRY_CAST_TEMPLATE(uint8_t, uint64_t)
TRY_CAST_TEMPLATE(uint8_t, float)
TRY_CAST_TEMPLATE(uint8_t, double)

//===--------------------------------------------------------------------===//
// uint16_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(uint16_t, bool)
TRY_CAST_TEMPLATE(uint16_t, int8_t)
TRY_CAST_TEMPLATE(uint16_t, int16_t)
TRY_CAST_TEMPLATE(uint16_t, int32_t)
TRY_CAST_TEMPLATE(uint16_t, int64_t)
TRY_CAST_TEMPLATE(uint16_t, uint8_t)
TRY_CAST_TEMPLATE(uint16_t, uint16_t)
TRY_CAST_TEMPLATE(uint16_t, uint32_t)
TRY_CAST_TEMPLATE(uint16_t, uint64_t)
TRY_CAST_TEMPLATE(uint16_t, float)
TRY_CAST_TEMPLATE(uint16_t, double)

//===--------------------------------------------------------------------===//
// uint32_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(uint32_t, bool)
TRY_CAST_TEMPLATE(uint32_t, int8_t)
TRY_CAST_TEMPLATE(uint32_t, int16_t)
TRY_CAST_TEMPLATE(uint32_t, int32_t)
TRY_CAST_TEMPLATE(uint32_t, int64_t)
TRY_CAST_TEMPLATE(uint32_t, uint8_t)
TRY_CAST_TEMPLATE(uint32_t, uint16_t)
TRY_CAST_TEMPLATE(uint32_t, uint32_t)
TRY_CAST_TEMPLATE(uint32_t, uint64_t)
TRY_CAST_TEMPLATE(uint32_t, float)
TRY_CAST_TEMPLATE(uint32_t, double)

//===--------------------------------------------------------------------===//
// uint64_t casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(uint64_t, bool)
TRY_CAST_TEMPLATE(uint64_t, int8_t)
TRY_CAST_TEMPLATE(uint64_t, int16_t)
TRY_CAST_TEMPLATE(uint64_t, int32_t)
TRY_CAST_TEMPLATE(uint64_t, int64_t)
TRY_CAST_TEMPLATE(uint64_t, uint8_t)
TRY_CAST_TEMPLATE(uint64_t, uint16_t)
TRY_CAST_TEMPLATE(uint64_t, uint32_t)
TRY_CAST_TEMPLATE(uint64_t, uint64_t)
TRY_CAST_TEMPLATE(uint64_t, float)
TRY_CAST_TEMPLATE(uint64_t, double)

//===--------------------------------------------------------------------===//
// float casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(float, bool)
TRY_CAST_TEMPLATE(float, int8_t)
TRY_CAST_TEMPLATE(float, int16_t)
TRY_CAST_TEMPLATE(float, int32_t)
TRY_CAST_TEMPLATE(float, int64_t)
TRY_CAST_TEMPLATE(float, uint8_t)
TRY_CAST_TEMPLATE(float, uint16_t)
TRY_CAST_TEMPLATE(float, uint32_t)
TRY_CAST_TEMPLATE(float, uint64_t)
TRY_CAST_TEMPLATE(float, float)
TRY_CAST_TEMPLATE(float, double)

//===--------------------------------------------------------------------===//
// double casts
//===--------------------------------------------------------------------===//
TRY_CAST_TEMPLATE(double, bool)
TRY_CAST_TEMPLATE(double, int8_t)
TRY_CAST_TEMPLATE(double, int16_t)
TRY_CAST_TEMPLATE(double, int32_t)
TRY_CAST_TEMPLATE(double, int64_t)
TRY_CAST_TEMPLATE(double, uint8_t)
TRY_CAST_TEMPLATE(double, uint16_t)
TRY_CAST_TEMPLATE(double, uint32_t)
TRY_CAST_TEMPLATE(double, uint64_t)
template <>
bool TryCast::Operation(double input, float &result, bool strict);
TRY_CAST_TEMPLATE(double, double)

//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, bool &result, bool strict);
template <>
bool TryCast::Operation(string_t input, int8_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, int16_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, int32_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, int64_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, uint8_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, uint16_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, uint32_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, uint64_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(string_t input, float &result, bool strict);
template <>
bool TryCast::Operation(string_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Hugeint casts
//===--------------------------------------------------------------------===//
// Numeric -> Hugeint casts
template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict);
template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict);

// Hugeint -> numeric casts
template <>
bool TryCast::Operation(hugeint_t input, bool &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, float &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, double &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, date_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, dtime_t &result, bool strict);
template <>
bool TryCast::Operation(hugeint_t input, timestamp_t &result, bool strict);

// nop cast
template <>
bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict);
template <>
hugeint_t Cast::Operation(hugeint_t input);

//===--------------------------------------------------------------------===//
// Date Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(date_t input, date_t &result, bool strict);
template <>
bool TryCast::Operation(date_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Time Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Timestamp Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(timestamp_t input, date_t &result, bool strict);
template <>
bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict);
template <>
bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// String -> Date Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, date_t &result, bool strict);
template <>
date_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Time Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, dtime_t &result, bool strict);
template <>
dtime_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Time Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, timestamp_t &result, bool strict);
template <>
timestamp_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Interval Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(string_t input, interval_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
struct CastToDecimal {
	template <class SRC, class DST>
	static inline DST Operation(SRC input, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for CastToDecimal!");
	}
};

struct CastFromDecimal {
	template <class SRC, class DST>
	static inline DST Operation(SRC input, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for CastFromDecimal!");
	}
};

// BOOLEAN
template <>
int16_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(bool input, uint8_t width, uint8_t scale);

template <>
bool CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
bool CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
bool CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
bool CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// TINYINT
template <>
int16_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);

template <>
int8_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
int8_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
int8_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
int8_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// SMALLINT
template <>
int16_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);

template <>
int16_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
int16_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
int16_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
int16_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// INTEGER
template <>
int16_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);

template <>
int32_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// BIGINT
template <>
int16_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);

template <>
int64_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// UTINYINT
template <>
int16_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(uint8_t input, uint8_t width, uint8_t scale);

template <>
uint8_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
uint8_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
uint8_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
uint8_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// USMALLINT
template <>
int16_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(uint16_t input, uint8_t width, uint8_t scale);

template <>
uint16_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
uint16_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
uint16_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
uint16_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// UINTEGER
template <>
int16_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(uint32_t input, uint8_t width, uint8_t scale);

template <>
uint32_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
uint32_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
uint32_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
uint32_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// BIGINT
template <>
int16_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(uint64_t input, uint8_t width, uint8_t scale);

template <>
uint64_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
uint64_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
uint64_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
uint64_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// HUGEINT
template <>
int16_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

template <>
hugeint_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// FLOAT
template <>
int16_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);

template <>
float CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
float CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
float CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
float CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// DOUBLE
template <>
int16_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);

template <>
double CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template <>
double CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template <>
double CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template <>
double CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// VARCHAR
template <>
int16_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);
template <>
int32_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);
template <>
int64_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);
template <>
hugeint_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);

struct StringCastFromDecimal {
	template <class SRC>
	static inline string_t Operation(SRC input, uint8_t width, uint8_t scale, Vector &result) {
		throw NotImplementedException("Unimplemented type for string cast!");
	}
};

template <>
string_t StringCastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale, Vector &result);
template <>
string_t StringCastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale, Vector &result);
template <>
string_t StringCastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale, Vector &result);
template <>
string_t StringCastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale, Vector &result);

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps
//===--------------------------------------------------------------------===//
struct CastToTimestampNS {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastToTimestampMS {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastToTimestampSec {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestampNS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestampMS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestampSec {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampUsToMs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampUsToNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampUsToSec {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampMsToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampNsToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampSecToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

template <>
duckdb::timestamp_t CastTimestampUsToMs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampUsToNs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampUsToSec::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampMsToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampNsToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToUs::Operation(duckdb::timestamp_t input);

template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result);
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result);
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result);

template <>
duckdb::timestamp_t CastToTimestampNS::Operation(string_t input);
template <>
duckdb::timestamp_t CastToTimestampMS::Operation(string_t input);
template <>
duckdb::timestamp_t CastToTimestampSec::Operation(string_t input);

//===--------------------------------------------------------------------===//
// Blobs
//===--------------------------------------------------------------------===//
struct CastFromBlob {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from blob could not be performed!");
	}
};
template <>
duckdb::string_t CastFromBlob::Operation(duckdb::string_t input, Vector &vector);

struct CastToBlob {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to blob could not be performed!");
	}
};
template <>
duckdb::string_t CastToBlob::Operation(duckdb::string_t input, Vector &vector);

} // namespace duckdb
