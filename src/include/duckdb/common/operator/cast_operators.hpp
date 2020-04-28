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
template <> bool TryCast::Operation(string_t input, float &result, bool strict);
template <> bool TryCast::Operation(string_t input, double &result, bool strict);

template <> bool Cast::Operation(string_t input);
template <> int8_t Cast::Operation(string_t input);
template <> int16_t Cast::Operation(string_t input);
template <> int32_t Cast::Operation(string_t input);
template <> int64_t Cast::Operation(string_t input);
template <> float Cast::Operation(string_t input);
template <> double Cast::Operation(string_t input);
template <> string Cast::Operation(string_t input);

template <> bool StrictCast::Operation(string_t input);
template <> int8_t StrictCast::Operation(string_t input);
template <> int16_t StrictCast::Operation(string_t input);
template <> int32_t StrictCast::Operation(string_t input);
template <> int64_t StrictCast::Operation(string_t input);
template <> float StrictCast::Operation(string_t input);
template <> double StrictCast::Operation(string_t input);
template <> string StrictCast::Operation(string_t input);
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
template <> duckdb::string_t StringCast::Operation(float input, Vector &result);
template <> duckdb::string_t StringCast::Operation(double input, Vector &result);

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

struct CastDateToTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};
template <> duckdb::string_t CastFromDate::Operation(duckdb::date_t input, Vector &result);
template <> duckdb::date_t CastToDate::Operation(string_t input);
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
template <> duckdb::string_t CastFromTime::Operation(duckdb::dtime_t input, Vector &result);
template <> duckdb::dtime_t CastToTime::Operation(string_t input);

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

} // namespace duckdb
