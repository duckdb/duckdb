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
#include "duckdb/common/types/null_value.hpp"

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


template<class SRC, class DST>
static string CastExceptionText(SRC input) {
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
			throw InvalidInputException(CastExceptionText<SRC, DST>(input));
		}
		return result;
	}
};

struct HandleCastError {
	static void AssignError(string error_message, string *error_message_ptr) {
		if (!error_message_ptr) {
			throw ConversionException(error_message);
		}
		if (error_message_ptr->empty()) {
			*error_message_ptr = error_message;
		}
	}
};

#define TRY_CAST_TEMPLATE(SOURCE_TYPE, TARGET_TYPE) \
template <> \
bool TryCast::Operation(SOURCE_TYPE input, TARGET_TYPE &result, bool strict)

#define NUMERIC_TRY_CAST_TEMPLATE(SOURCE_TYPE) \
TRY_CAST_TEMPLATE(SOURCE_TYPE, bool); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, int8_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, int16_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, int32_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, int64_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, hugeint_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, uint8_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, uint16_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, uint32_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, uint64_t); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, float); \
TRY_CAST_TEMPLATE(SOURCE_TYPE, double);

//===--------------------------------------------------------------------===//
// Numeric cast templates
//===--------------------------------------------------------------------===//
NUMERIC_TRY_CAST_TEMPLATE(bool)
NUMERIC_TRY_CAST_TEMPLATE(int8_t)
NUMERIC_TRY_CAST_TEMPLATE(int16_t)
NUMERIC_TRY_CAST_TEMPLATE(int32_t)
NUMERIC_TRY_CAST_TEMPLATE(int64_t)
NUMERIC_TRY_CAST_TEMPLATE(hugeint_t)
NUMERIC_TRY_CAST_TEMPLATE(uint8_t)
NUMERIC_TRY_CAST_TEMPLATE(uint16_t)
NUMERIC_TRY_CAST_TEMPLATE(uint32_t)
NUMERIC_TRY_CAST_TEMPLATE(uint64_t)
NUMERIC_TRY_CAST_TEMPLATE(float)
NUMERIC_TRY_CAST_TEMPLATE(double)

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
// Interval Casts
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(interval_t input, interval_t &result, bool strict);

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
bool TryCastErrorMessage::Operation(string_t input, interval_t &result, string *error_message, bool strict);

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
struct TryCastToDecimal {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for TryCastToDecimal!");
	}
};

struct TryCastFromDecimal {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for TryCastFromDecimal!");
	}
};

#define TRY_CAST_TO_DECIMAL_TEMPLATE(SOURCE_TYPE) \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, int16_t &result, string *error_message, uint8_t width, uint8_t scale); \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, int32_t &result, string *error_message, uint8_t width, uint8_t scale); \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, int64_t &result, string *error_message, uint8_t width, uint8_t scale); \
template <> \
bool TryCastToDecimal::Operation(SOURCE_TYPE input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale)


#define TRY_CAST_FROM_DECIMAL_TEMPLATE(TARGET_TYPE) \
template <> \
bool TryCastFromDecimal::Operation(int16_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale); \
template <> \
bool TryCastFromDecimal::Operation(int32_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale); \
template <> \
bool TryCastFromDecimal::Operation(int64_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale); \
template <> \
bool TryCastFromDecimal::Operation(hugeint_t input, TARGET_TYPE &result, string *error_message, uint8_t width, uint8_t scale)

// BOOLEAN
TRY_CAST_TO_DECIMAL_TEMPLATE(bool);
TRY_CAST_FROM_DECIMAL_TEMPLATE(bool);

// TINYINT
TRY_CAST_TO_DECIMAL_TEMPLATE(int8_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(int8_t);

// SMALLINT
TRY_CAST_TO_DECIMAL_TEMPLATE(int16_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(int16_t);

// INTEGER
TRY_CAST_TO_DECIMAL_TEMPLATE(int32_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(int32_t);

// BIGINT
TRY_CAST_TO_DECIMAL_TEMPLATE(int64_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(int64_t);

// UTINYINT
TRY_CAST_TO_DECIMAL_TEMPLATE(uint8_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(uint8_t);

// USMALLINT
TRY_CAST_TO_DECIMAL_TEMPLATE(uint16_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(uint16_t);

// UINTEGER
TRY_CAST_TO_DECIMAL_TEMPLATE(uint32_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(uint32_t);

// BIGINT
TRY_CAST_TO_DECIMAL_TEMPLATE(uint64_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(uint64_t);

// HUGEINT
TRY_CAST_TO_DECIMAL_TEMPLATE(hugeint_t);
TRY_CAST_FROM_DECIMAL_TEMPLATE(hugeint_t);

// FLOAT
TRY_CAST_TO_DECIMAL_TEMPLATE(float);
TRY_CAST_FROM_DECIMAL_TEMPLATE(float);

// DOUBLE
TRY_CAST_TO_DECIMAL_TEMPLATE(double);
TRY_CAST_FROM_DECIMAL_TEMPLATE(double);

// VARCHAR
TRY_CAST_TO_DECIMAL_TEMPLATE(string_t);

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
bool TryCastToTimestampNS::Operation(string_t input, timestamp_t &result, bool strict);
template <>
bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict);
template <>
bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps -> string/standard timestamp
//===--------------------------------------------------------------------===//

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

struct TryCastToBlob {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, string *error_message, bool strict = false) {
		throw InternalException("Unsupported type for try cast to blob");
	}
};

template <>
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message, bool strict);

} // namespace duckdb
