//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "add.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/convert_to_string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "multiply.hpp"
#include "subtract.hpp"

namespace duckdb {
struct ValidityMask;
class Vector;

struct TryCast {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw NotImplementedException("Unimplemented type for cast (%s -> %s)", GetTypeId<SRC>(), GetTypeId<DST>());
	}
};

struct TryCastErrorMessage {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, string *error_message, bool strict = false) {
		throw NotImplementedException("Unimplemented type for cast (%s -> %s)", GetTypeId<SRC>(), GetTypeId<DST>());
	}
};

struct TryCastErrorMessageCommaSeparated {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, string *error_message, bool strict = false) {
		throw NotImplementedException("Unimplemented type for cast (%s -> %s)", GetTypeId<SRC>(), GetTypeId<DST>());
	}
};

template <class SRC, class DST>
static string CastExceptionText(SRC input) {
	if (std::is_same<SRC, string_t>()) {
		return "Could not convert string '" + ConvertToString::Operation<SRC>(input) + "' to " +
		       TypeIdToString(GetTypeId<DST>());
	}
	if (TypeIsNumber<SRC>() && TypeIsNumber<DST>()) {
		return "Type " + TypeIdToString(GetTypeId<SRC>()) + " with value " + ConvertToString::Operation<SRC>(input) +
		       " can't be cast because the value is out of range for the destination type " +
		       TypeIdToString(GetTypeId<DST>());
	}
	return "Type " + TypeIdToString(GetTypeId<SRC>()) + " with value " + ConvertToString::Operation<SRC>(input) +
	       " can't be cast to the destination type " + TypeIdToString(GetTypeId<DST>());
}

struct Cast {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		DST result;
		if (!TryCast::Operation(input, result)) {
			throw InvalidInputException(CastExceptionText<SRC, DST>(input));
		}
		return result;
	}
};

struct HandleCastError {
	static void AssignError(const string &error_message, string *error_message_ptr) {
		if (!error_message_ptr) {
			throw ConversionException(error_message);
		}
		if (error_message_ptr->empty()) {
			*error_message_ptr = error_message;
		}
	}
};

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(bool input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(bool input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int8_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int16_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int32_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(int64_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(hugeint_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uhugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uhugeint_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint8_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint16_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint32_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(uint64_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(float input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(float input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(double input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(double input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(string_t input, bool &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, int64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint8_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint16_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint32_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uint64_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, hugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, uhugeint_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, float &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, double &result, bool strict);
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, float &result, string *error_message, bool strict);
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, double &result, string *error_message, bool strict);
template <>
DUCKDB_API bool TryCastErrorMessageCommaSeparated::Operation(string_t input, float &result, string *error_message,
                                                             bool strict);
template <>
DUCKDB_API bool TryCastErrorMessageCommaSeparated::Operation(string_t input, double &result, string *error_message,
                                                             bool strict);

//===--------------------------------------------------------------------===//
// Date Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(date_t input, date_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(date_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Time Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(dtime_t input, dtime_tz_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Time With Time Zone Casts (Offset)
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(dtime_tz_t input, dtime_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(dtime_tz_t input, dtime_tz_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Timestamp Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, date_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, dtime_tz_t &result, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Interval Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCast::Operation(interval_t input, interval_t &result, bool strict);

//===--------------------------------------------------------------------===//
// String -> Date Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, date_t &result, string *error_message, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, date_t &result, bool strict);
template <>
date_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Time Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, dtime_t &result, string *error_message, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, dtime_t &result, bool strict);
template <>
dtime_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> TimeTZ Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, dtime_tz_t &result, string *error_message, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, dtime_tz_t &result, bool strict);
template <>
dtime_tz_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Timestamp Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, timestamp_t &result, string *error_message, bool strict);
template <>
DUCKDB_API bool TryCast::Operation(string_t input, timestamp_t &result, bool strict);
template <>
timestamp_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// String -> Interval Casts
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastErrorMessage::Operation(string_t input, interval_t &result, string *error_message, bool strict);

//===--------------------------------------------------------------------===//
// string -> Non-Standard Timestamps
//===--------------------------------------------------------------------===//
struct TryCastToTimestampNS {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to timestamp (ns)");
	}
};

struct TryCastToTimestampMS {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to timestamp (ms)");
	}
};

struct TryCastToTimestampSec {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		throw InternalException("Unsupported type for try cast to timestamp (s)");
	}
};

template <>
DUCKDB_API bool TryCastToTimestampNS::Operation(string_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict);

template <>
DUCKDB_API bool TryCastToTimestampNS::Operation(date_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampMS::Operation(date_t input, timestamp_t &result, bool strict);
template <>
DUCKDB_API bool TryCastToTimestampSec::Operation(date_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps -> string/timestamp types
//===--------------------------------------------------------------------===//

struct CastFromTimestampNS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
	}
};

struct CastFromTimestampMS {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
	}
};

struct CastFromTimestampSec {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to string could not be performed!");
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

struct CastTimestampMsToDate {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to DATE could not be performed!");
	}
};

struct CastTimestampMsToTime {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME could not be performed!");
	}
};

struct CastTimestampMsToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampMsToNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIMESTAMP_NS could not be performed!");
	}
};

struct CastTimestampNsToDate {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to DATE could not be performed!");
	}
};
struct CastTimestampNsToTime {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME could not be performed!");
	}
};
struct CastTimestampNsToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampSecToDate {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to DATE could not be performed!");
	}
};
struct CastTimestampSecToTime {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIME could not be performed!");
	}
};
struct CastTimestampSecToMs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIMESTAMP_MS could not be performed!");
	}
};

struct CastTimestampSecToUs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampSecToNs {
	template <class SRC, class DST>
	static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to TIMESTAMP_NS could not be performed!");
	}
};

template <>
duckdb::timestamp_t CastTimestampUsToSec::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampUsToMs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampUsToNs::Operation(duckdb::timestamp_t input);
template <>
duckdb::date_t CastTimestampMsToDate::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_t CastTimestampMsToTime::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampMsToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampMsToNs::Operation(duckdb::timestamp_t input);
template <>
duckdb::date_t CastTimestampNsToDate::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_t CastTimestampNsToTime::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampNsToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::date_t CastTimestampSecToDate::Operation(duckdb::timestamp_t input);
template <>
duckdb::dtime_t CastTimestampSecToTime::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToMs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToUs::Operation(duckdb::timestamp_t input);
template <>
duckdb::timestamp_t CastTimestampSecToNs::Operation(duckdb::timestamp_t input);

template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result);
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result);
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result);

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

struct CastFromBlobToBit {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw NotImplementedException("Cast from blob could not be performed!");
	}
};
template <>
string_t CastFromBlobToBit::Operation(string_t input, Vector &result);

struct TryCastToBlob {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, string *error_message,
	                             bool strict = false) {
		throw InternalException("Unsupported type for try cast to blob");
	}
};
template <>
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message,
                              bool strict);

//===--------------------------------------------------------------------===//
// Bits
//===--------------------------------------------------------------------===//
struct CastFromBitToString {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from bit could not be performed!");
	}
};
template <>
duckdb::string_t CastFromBitToString::Operation(duckdb::string_t input, Vector &vector);

struct CastFromBitToNumeric {
	template <class SRC = string_t, class DST>
	static inline bool Operation(SRC input, DST &result, bool strict = false) {
		D_ASSERT(input.GetSize() > 1);

		// TODO: Allow conversion if the significant bytes of the bitstring can be cast to the target type
		// Currently only allows bitstring -> numeric if the full bitstring fits inside the numeric type
		if (input.GetSize() - 1 > sizeof(DST)) {
			throw ConversionException("Bitstring doesn't fit inside of %s", GetTypeId<DST>());
		}
		Bit::BitToNumeric(input, result);
		return (true);
	}
};
template <>
bool CastFromBitToNumeric::Operation(string_t input, bool &result, bool strict);
template <>
bool CastFromBitToNumeric::Operation(string_t input, hugeint_t &result, bool strict);
template <>
bool CastFromBitToNumeric::Operation(string_t input, uhugeint_t &result, bool strict);

struct CastFromBitToBlob {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		D_ASSERT(input.GetSize() > 1);
		return StringVector::AddStringOrBlob(result, Bit::BitToBlob(input));
	}
};

struct TryCastToBit {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, string *error_message,
	                             bool strict = false) {
		throw InternalException("Unsupported type for try cast to bit");
	}
};

template <>
bool TryCastToBit::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message,
                             bool strict);

//===--------------------------------------------------------------------===//
// UUID
//===--------------------------------------------------------------------===//
struct CastFromUUID {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from uuid could not be performed!");
	}
};
template <>
duckdb::string_t CastFromUUID::Operation(duckdb::hugeint_t input, Vector &vector);

struct TryCastToUUID {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, string *error_message,
	                             bool strict = false) {
		throw InternalException("Unsupported type for try cast to uuid");
	}
};

template <>
DUCKDB_API bool TryCastToUUID::Operation(string_t input, hugeint_t &result, Vector &result_vector,
                                         string *error_message, bool strict);

//===--------------------------------------------------------------------===//
// Pointers
//===--------------------------------------------------------------------===//
struct CastFromPointer {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from pointer could not be performed!");
	}
};
template <>
duckdb::string_t CastFromPointer::Operation(uintptr_t input, Vector &vector);

template <typename T>
struct IntegerCastData {
	using ResultType = T;
	using StoreType = T;
	ResultType result;
};

struct IntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		using store_t = typename T::StoreType;
		if (NEGATIVE) {
			if (DUCKDB_UNLIKELY(state.result < (NumericLimits<store_t>::Minimum() + digit) / 10)) {
				return false;
			}
			state.result = state.result * 10 - digit;
		} else {
			if (DUCKDB_UNLIKELY(state.result > (NumericLimits<store_t>::Maximum() - digit) / 10)) {
				return false;
			}
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &state, uint8_t digit) {
		using store_t = typename T::StoreType;
		if (DUCKDB_UNLIKELY(state.result > (NumericLimits<store_t>::Maximum() - digit) / 16)) {
			return false;
		}
		state.result = state.result * 16 + digit;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &state, uint8_t digit) {
		using store_t = typename T::StoreType;
		if (DUCKDB_UNLIKELY(state.result > (NumericLimits<store_t>::Maximum() - digit) / 2)) {
			return false;
		}
		state.result = state.result * 2 + digit;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int16_t exponent) {
		// Simple integers don't deal with Exponents
		return false;
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		// Simple integers don't deal with Decimals
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		return true;
	}
};

template <typename T>
struct IntegerDecimalCastData {
	using ResultType = T;
	using StoreType = int64_t;
	StoreType result;
	StoreType decimal;
	uint16_t decimal_digits;
};

template <>
struct IntegerDecimalCastData<uint64_t> {
	using ResultType = uint64_t;
	using StoreType = uint64_t;
	StoreType result;
	StoreType decimal;
	uint16_t decimal_digits;
};

struct IntegerDecimalCastOperation : IntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int16_t exponent) {
		using store_t = typename T::StoreType;

		int16_t e = exponent;
		// Negative Exponent
		if (e < 0) {
			while (state.result != 0 && e++ < 0) {
				state.decimal = state.result % 10;
				state.result /= 10;
			}
			if (state.decimal < 0) {
				state.decimal = -state.decimal;
			}
			state.decimal_digits = 1;
			return Finalize<T, NEGATIVE>(state);
		}

		// Positive Exponent
		while (state.result != 0 && e-- > 0) {
			if (!TryMultiplyOperator::Operation(state.result, (store_t)10, state.result)) {
				return false;
			}
		}

		if (state.decimal == 0) {
			return Finalize<T, NEGATIVE>(state);
		}

		// Handle decimals
		e = exponent - state.decimal_digits;
		store_t remainder = 0;
		if (e < 0) {
			if (static_cast<uint16_t>(-e) <= NumericLimits<store_t>::Digits()) {
				store_t power = 1;
				while (e++ < 0) {
					power *= 10;
				}
				remainder = state.decimal % power;
				state.decimal /= power;
			} else {
				state.decimal = 0;
			}
		} else {
			while (e-- > 0) {
				if (!TryMultiplyOperator::Operation(state.decimal, (store_t)10, state.decimal)) {
					return false;
				}
			}
		}

		state.decimal_digits -= exponent;

		if (NEGATIVE) {
			if (!TrySubtractOperator::Operation(state.result, state.decimal, state.result)) {
				return false;
			}
		} else if (!TryAddOperator::Operation(state.result, state.decimal, state.result)) {
			return false;
		}
		state.decimal = remainder;
		return Finalize<T, NEGATIVE>(state);
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		using store_t = typename T::StoreType;
		if (DUCKDB_UNLIKELY(state.decimal > (NumericLimits<store_t>::Maximum() - digit) / 10)) {
			// Simply ignore any more decimals
			return true;
		}
		state.decimal_digits++;
		state.decimal = state.decimal * 10 + digit;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		using result_t = typename T::ResultType;
		using store_t = typename T::StoreType;

		result_t tmp;
		if (!TryCast::Operation<store_t, result_t>(state.result, tmp)) {
			return false;
		}

		while (state.decimal > 10) {
			state.decimal /= 10;
			state.decimal_digits--;
		}

		bool success = true;
		if (state.decimal_digits == 1 && state.decimal >= 5) {
			if (NEGATIVE) {
				success = TrySubtractOperator::Operation(tmp, (result_t)1, tmp);
			} else {
				success = TryAddOperator::Operation(tmp, (result_t)1, tmp);
			}
		}
		state.result = tmp;
		return success;
	}
};

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation, char decimal_separator = '.'>
static bool IntegerCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	idx_t start_pos;
	if (NEGATIVE) {
		start_pos = 1;
	} else {
		if (*buf == '+') {
			if (strict) {
				// leading plus is not allowed in strict mode
				return false;
			}
			start_pos = 1;
		} else {
			start_pos = 0;
		}
	}
	idx_t pos = start_pos;
	while (pos < len) {
		if (!StringUtil::CharacterIsDigit(buf[pos])) {
			// not a digit!
			if (buf[pos] == decimal_separator) {
				if (strict) {
					return false;
				}
				bool number_before_period = pos > start_pos;
				// decimal point: we accept decimal values for integers as well
				// we just truncate them
				// make sure everything after the period is a number
				pos++;
				idx_t start_digit = pos;
				while (pos < len) {
					if (!StringUtil::CharacterIsDigit(buf[pos])) {
						break;
					}
					if (!OP::template HandleDecimal<T, NEGATIVE, ALLOW_EXPONENT>(result, buf[pos] - '0')) {
						return false;
					}
					pos++;
				}
				// make sure there is either (1) one number after the period, or (2) one number before the period
				// i.e. we accept "1." and ".1" as valid numbers, but not "."
				if (!(number_before_period || pos > start_digit)) {
					return false;
				}
				if (pos >= len) {
					break;
				}
			}
			if (StringUtil::CharacterIsSpace(buf[pos])) {
				// skip any trailing spaces
				while (++pos < len) {
					if (!StringUtil::CharacterIsSpace(buf[pos])) {
						return false;
					}
				}
				break;
			}
			if (ALLOW_EXPONENT) {
				if (buf[pos] == 'e' || buf[pos] == 'E') {
					if (pos == start_pos) {
						return false;
					}
					pos++;
					if (pos >= len) {
						return false;
					}
					using ExponentData = IntegerCastData<int16_t>;
					ExponentData exponent {};
					int negative = buf[pos] == '-';
					if (negative) {
						if (!IntegerCastLoop<ExponentData, true, false, IntegerCastOperation, decimal_separator>(
						        buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					} else {
						if (!IntegerCastLoop<ExponentData, false, false, IntegerCastOperation, decimal_separator>(
						        buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					}
					return OP::template HandleExponent<T, NEGATIVE>(result, exponent.result);
				}
			}
			return false;
		}
		uint8_t digit = buf[pos++] - '0';
		if (!OP::template HandleDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T, NEGATIVE>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerHexCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	if (ALLOW_EXPONENT || NEGATIVE) {
		return false;
	}
	idx_t start_pos = 1;
	idx_t pos = start_pos;
	char current_char;
	while (pos < len) {
		current_char = StringUtil::CharacterToLower(buf[pos]);
		if (!StringUtil::CharacterIsHex(current_char)) {
			return false;
		}
		uint8_t digit;
		if (current_char >= 'a') {
			digit = current_char - 'a' + 10;
		} else {
			digit = current_char - '0';
		}
		pos++;
		if (!OP::template HandleHexDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T, NEGATIVE>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerBinaryCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	if (ALLOW_EXPONENT || NEGATIVE) {
		return false;
	}
	idx_t start_pos = 1;
	idx_t pos = start_pos;
	uint8_t digit;
	char current_char;
	while (pos < len) {
		current_char = buf[pos];
		if (current_char == '_' && pos > start_pos) {
			// skip underscore, if it is not the first character
			pos++;
			if (pos == len) {
				// we cant end on an underscore either
				return false;
			}
			continue;
		} else if (current_char == '0') {
			digit = 0;
		} else if (current_char == '1') {
			digit = 1;
		} else {
			return false;
		}
		pos++;
		if (!OP::template HandleBinaryDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T, NEGATIVE>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool IS_SIGNED = true, bool ALLOW_EXPONENT = true, class OP = IntegerCastOperation,
          bool ZERO_INITIALIZE = true, char decimal_separator = '.'>
static bool TryIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	if (ZERO_INITIALIZE) {
		memset(&result, 0, sizeof(T));
	}
	// if the number is negative, we set the negative flag and skip the negative sign
	if (*buf == '-') {
		if (!IS_SIGNED) {
			// Need to check if its not -0
			idx_t pos = 1;
			while (pos < len) {
				if (buf[pos++] != '0') {
					return false;
				}
			}
		}
		return IntegerCastLoop<T, true, ALLOW_EXPONENT, OP, decimal_separator>(buf, len, result, strict);
	}
	if (len > 1 && *buf == '0') {
		if (buf[1] == 'x' || buf[1] == 'X') {
			// If it starts with 0x or 0X, we parse it as a hex value
			buf++;
			len--;
			return IntegerHexCastLoop<T, false, false, OP>(buf, len, result, strict);
		} else if (buf[1] == 'b' || buf[1] == 'B') {
			// If it starts with 0b or 0B, we parse it as a binary value
			buf++;
			len--;
			return IntegerBinaryCastLoop<T, false, false, OP>(buf, len, result, strict);
		} else if (strict && StringUtil::CharacterIsDigit(buf[1])) {
			// leading zeros are not allowed in strict mode
			return false;
		}
	}
	return IntegerCastLoop<T, false, ALLOW_EXPONENT, OP, decimal_separator>(buf, len, result, strict);
}

template <typename T, bool IS_SIGNED = true>
static inline bool TrySimpleIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
	IntegerCastData<T> simple_data;
	if (TryIntegerCast<IntegerCastData<T>, IS_SIGNED, false, IntegerCastOperation>(buf, len, simple_data, strict)) {
		result = (T)simple_data.result;
		return true;
	}

	// Simple integer cast failed, try again with decimals/exponents included
	// FIXME: This could definitely be improved as some extra work is being done here. It is more important that
	//  "normal" integers (without exponent/decimals) are still being parsed quickly.
	IntegerDecimalCastData<T> cast_data;
	if (TryIntegerCast<IntegerDecimalCastData<T>, IS_SIGNED, true, IntegerDecimalCastOperation>(buf, len, cast_data,
	                                                                                            strict)) {
		result = (T)cast_data.result;
		return true;
	}
	return false;
}

} // namespace duckdb
