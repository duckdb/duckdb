//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct Cast {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		return (DST)input;
	}
};

struct TryCast {
	template <class SRC, class DST> static inline bool Operation(SRC input, DST &target, bool strict = false) {
		target = Cast::Operation(input);
		return true;
	}
};

struct StrictCast {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		return (DST)input;
	}
};

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int16_t input, int8_t &result, bool strict);
template <> bool TryCast::Operation(int32_t input, int8_t &result, bool strict);
template <> bool TryCast::Operation(int64_t input, int8_t &result, bool strict);
template <> bool TryCast::Operation(float input, int8_t &result, bool strict);
template <> bool TryCast::Operation(double input, int8_t &result, bool strict);

template <> int8_t Cast::Operation(int16_t input);
template <> int8_t Cast::Operation(int32_t input);
template <> int8_t Cast::Operation(int64_t input);
template <> int8_t Cast::Operation(float input);
template <> int8_t Cast::Operation(double input);

//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int32_t input, int16_t &result, bool strict);
template <> bool TryCast::Operation(int64_t input, int16_t &result, bool strict);
template <> bool TryCast::Operation(float input, int16_t &result, bool strict);
template <> bool TryCast::Operation(double input, int16_t &result, bool strict);

template <> int16_t Cast::Operation(int32_t input);
template <> int16_t Cast::Operation(int64_t input);
template <> int16_t Cast::Operation(float input);
template <> int16_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int64_t input, int32_t &result, bool strict);
template <> bool TryCast::Operation(float input, int32_t &result, bool strict);
template <> bool TryCast::Operation(double input, int32_t &result, bool strict);

template <> int32_t Cast::Operation(int64_t input);
template <> int32_t Cast::Operation(float input);
template <> int32_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(float input, int64_t &result, bool strict);
template <> bool TryCast::Operation(double input, int64_t &result, bool strict);

template <> int64_t Cast::Operation(float input);
template <> int64_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// Double -> float casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(double input, float &result, bool strict);

template <> float Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(string_t input, bool &result, bool strict);
template <> bool TryCast::Operation(string_t input, int8_t &result, bool strict);
template <> bool TryCast::Operation(string_t input, int16_t &result, bool strict);
template <> bool TryCast::Operation(string_t input, int32_t &result, bool strict);
template <> bool TryCast::Operation(string_t input, int64_t &result, bool strict);
template <> bool TryCast::Operation(string_t input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(string_t input, float &result, bool strict);
template <> bool TryCast::Operation(string_t input, double &result, bool strict);

template <> bool Cast::Operation(string_t input);
template <> int8_t Cast::Operation(string_t input);
template <> int16_t Cast::Operation(string_t input);
template <> int32_t Cast::Operation(string_t input);
template <> int64_t Cast::Operation(string_t input);
template <> hugeint_t Cast::Operation(string_t input);
template <> float Cast::Operation(string_t input);
template <> double Cast::Operation(string_t input);
template <> string Cast::Operation(string_t input);

template <> bool StrictCast::Operation(string_t input);
template <> int8_t StrictCast::Operation(string_t input);
template <> int16_t StrictCast::Operation(string_t input);
template <> int32_t StrictCast::Operation(string_t input);
template <> int64_t StrictCast::Operation(string_t input);
template <> hugeint_t StrictCast::Operation(string_t input);
template <> float StrictCast::Operation(string_t input);
template <> double StrictCast::Operation(string_t input);
template <> string StrictCast::Operation(string_t input);

//===--------------------------------------------------------------------===//
// Hugeint casts
//===--------------------------------------------------------------------===//
// Numeric -> Hugeint casts
template <> bool TryCast::Operation(bool input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(float input, hugeint_t &result, bool strict);
template <> bool TryCast::Operation(double input, hugeint_t &result, bool strict);

template <> hugeint_t Cast::Operation(bool input);
template <> hugeint_t Cast::Operation(int8_t input);
template <> hugeint_t Cast::Operation(int16_t input);
template <> hugeint_t Cast::Operation(int32_t input);
template <> hugeint_t Cast::Operation(int64_t input);
template <> hugeint_t Cast::Operation(float input);
template <> hugeint_t Cast::Operation(double input);
// Hugeint -> numeric casts
template <> bool TryCast::Operation(hugeint_t input, bool &result, bool strict);
template <> bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict);
template <> bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict);
template <> bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict);
template <> bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict);
template <> bool TryCast::Operation(hugeint_t input, float &result, bool strict);
template <> bool TryCast::Operation(hugeint_t input, double &result, bool strict);

template <> bool Cast::Operation(hugeint_t input);
template <> int8_t Cast::Operation(hugeint_t input);
template <> int16_t Cast::Operation(hugeint_t input);
template <> int32_t Cast::Operation(hugeint_t input);
template <> int64_t Cast::Operation(hugeint_t input);
template <> float Cast::Operation(hugeint_t input);
template <> double Cast::Operation(hugeint_t input);
// nop cast
template <> bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict);
template <> hugeint_t Cast::Operation(hugeint_t input);


//===--------------------------------------------------------------------===//
// Interval -> String Casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(string_t input, interval_t &result, bool strict);
template <> interval_t StrictCast::Operation(string_t input);
template <> interval_t Cast::Operation(string_t input);
//===--------------------------------------------------------------------===//
// Numeric -> String Casts
//===--------------------------------------------------------------------===//
// these functions are convenience functions that cast a value to a std::string, they are very slow
// for performance sensitive casting StringCast::Operation should be used
template <> string Cast::Operation(bool input);
template <> string Cast::Operation(int8_t input);
template <> string Cast::Operation(int16_t input);
template <> string Cast::Operation(int32_t input);
template <> string Cast::Operation(int64_t input);
template <> string Cast::Operation(hugeint_t input);
template <> string Cast::Operation(float input);
template <> string Cast::Operation(double input);
template <> string Cast::Operation(string_t input);

class Vector;
struct StringCast {
	template <class SRC> static inline string_t Operation(SRC input, Vector &result) {
		throw NotImplementedException("Unimplemented type for string cast!");
	}
};

template <> duckdb::string_t StringCast::Operation(bool input, Vector &result);
template <> duckdb::string_t StringCast::Operation(int8_t input, Vector &result);
template <> duckdb::string_t StringCast::Operation(int16_t input, Vector &result);
template <> duckdb::string_t StringCast::Operation(int32_t input, Vector &result);
template <> duckdb::string_t StringCast::Operation(int64_t input, Vector &result);
template <> duckdb::string_t StringCast::Operation(uint64_t input, Vector &result);
template <> duckdb::string_t StringCast::Operation(hugeint_t input, Vector &result);
template <> duckdb::string_t StringCast::Operation(float input, Vector &result);
template <> duckdb::string_t StringCast::Operation(double input, Vector &result);
template <> duckdb::string_t StringCast::Operation(interval_t input, Vector &result);

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
struct CastToDecimal {
	template<class SRC, class DST>
	static inline DST Operation(SRC input, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for CastToDecimal!");
	}
};

struct CastFromDecimal {
	template<class SRC, class DST>
	static inline DST Operation(SRC input, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for CastFromDecimal!");
	}
};

// TINYINT
template<> int16_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(int8_t input, uint8_t width, uint8_t scale);

template<> int8_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> int8_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> int8_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> int8_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// SMALLINT
template<> int16_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);

template<> int16_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> int16_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> int16_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> int16_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// INTEGER
template<> int16_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);

template<> int32_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> int32_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> int32_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> int32_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// BIGINT
template<> int16_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);

template<> int64_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> int64_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> int64_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> int64_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// HUGEINT
template<> int16_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

template<> hugeint_t CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// FLOAT
template<> int16_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(float input, uint8_t width, uint8_t scale);

template<> float CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> float CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> float CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> float CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// DOUBLE
template<> int16_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(double input, uint8_t width, uint8_t scale);

template<> double CastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale);
template<> double CastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale);
template<> double CastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale);
template<> double CastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale);

// VARCHAR
template<> int16_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);
template<> int32_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);
template<> int64_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);
template<> hugeint_t CastToDecimal::Operation(string_t input, uint8_t width, uint8_t scale);

struct StringCastFromDecimal {
	template <class SRC> static inline string_t Operation(SRC input, uint8_t width, uint8_t scale, Vector &result) {
		throw NotImplementedException("Unimplemented type for string cast!");
	}
};

template<> string_t StringCastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale, Vector &result);
template<> string_t StringCastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale, Vector &result);
template<> string_t StringCastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale, Vector &result);
template<> string_t StringCastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale, Vector &result);


//===--------------------------------------------------------------------===//
// Date Casts
//===--------------------------------------------------------------------===//
struct CastFromDate {
	template <class SRC> static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from date could not be performed!");
	}
};

struct CastToDate {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to date could not be performed!");
	}
};

struct StrictCastToDate {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to date could not be performed!");
	}
};

struct CastDateToTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};
template <> duckdb::string_t CastFromDate::Operation(duckdb::date_t input, Vector &result);
template <> duckdb::date_t CastToDate::Operation(string_t input);
template <> duckdb::date_t StrictCastToDate::Operation(string_t input);
template <> duckdb::timestamp_t CastDateToTimestamp::Operation(duckdb::date_t input);

struct CastFromTime {
	template <class SRC> static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from time could not be performed!");
	}
};
struct CastToTime {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to time could not be performed!");
	}
};
struct StrictCastToTime {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to time could not be performed!");
	}
};
template <> duckdb::string_t CastFromTime::Operation(duckdb::dtime_t input, Vector &result);
template <> duckdb::dtime_t CastToTime::Operation(string_t input);
template <> duckdb::dtime_t StrictCastToTime::Operation(string_t input);

struct CastToTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestamp {
	template <class SRC> static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampToDate {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastTimestampToTime {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

template <> duckdb::date_t CastTimestampToDate::Operation(duckdb::timestamp_t input);
template <> duckdb::dtime_t CastTimestampToTime::Operation(duckdb::timestamp_t input);
template <> duckdb::string_t CastFromTimestamp::Operation(duckdb::timestamp_t input, Vector &result);
template <> duckdb::timestamp_t CastToTimestamp::Operation(string_t input);

struct CastFromBlob {
	template <class SRC> static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast from blob could not be performed!");
	}

	static void ToHexString(duckdb::string_t input, duckdb::string_t &output);
	static void FromHexToBytes(duckdb::string_t input, duckdb::string_t &output);
};
template <> duckdb::string_t CastFromBlob::Operation(duckdb::string_t input, Vector &vector);

struct CastToBlob {
	template <class SRC> static inline string_t Operation(SRC input, Vector &result) {
		throw duckdb::NotImplementedException("Cast to blob could not be performed!");
	}
};
template <> duckdb::string_t CastToBlob::Operation(duckdb::string_t input, Vector &vector);

} // namespace duckdb
