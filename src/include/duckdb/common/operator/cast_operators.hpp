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

namespace duckdb {

struct Cast {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		return (DST)input;
	}
};

struct TryCast {
	template <class SRC, class DST> static inline bool Operation(SRC input, DST &target) {
		target = Cast::Operation(input);
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int16_t input, int8_t &result);
template <> bool TryCast::Operation(int32_t input, int8_t &result);
template <> bool TryCast::Operation(int64_t input, int8_t &result);
template <> bool TryCast::Operation(float input, int8_t &result);
template <> bool TryCast::Operation(double input, int8_t &result);

template <> int8_t Cast::Operation(int16_t input);
template <> int8_t Cast::Operation(int32_t input);
template <> int8_t Cast::Operation(int64_t input);
template <> int8_t Cast::Operation(float input);
template <> int8_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int32_t input, int16_t &result);
template <> bool TryCast::Operation(int64_t input, int16_t &result);
template <> bool TryCast::Operation(float input, int16_t &result);
template <> bool TryCast::Operation(double input, int16_t &result);

template <> int16_t Cast::Operation(int32_t input);
template <> int16_t Cast::Operation(int64_t input);
template <> int16_t Cast::Operation(float input);
template <> int16_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int64_t input, int32_t &result);
template <> bool TryCast::Operation(float input, int32_t &result);
template <> bool TryCast::Operation(double input, int32_t &result);

template <> int32_t Cast::Operation(int64_t input);
template <> int32_t Cast::Operation(float input);
template <> int32_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(float input, int64_t &result);
template <> bool TryCast::Operation(double input, int64_t &result);

template <> int64_t Cast::Operation(float input);
template <> int64_t Cast::Operation(double input);
//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(const char *input, bool &result);
template <> bool TryCast::Operation(const char *input, int8_t &result);
template <> bool TryCast::Operation(const char *input, int16_t &result);
template <> bool TryCast::Operation(const char *input, int32_t &result);
template <> bool TryCast::Operation(const char *input, int64_t &result);
template <> bool TryCast::Operation(const char *input, float &result);
template <> bool TryCast::Operation(const char *input, double &result);

template <> bool Cast::Operation(const char *input);
template <> int8_t Cast::Operation(const char *input);
template <> int16_t Cast::Operation(const char *input);
template <> int32_t Cast::Operation(const char *input);
template <> int64_t Cast::Operation(const char *input);
template <> float Cast::Operation(const char *input);
template <> double Cast::Operation(const char *input);
//===--------------------------------------------------------------------===//
// Numeric -> String Casts
//===--------------------------------------------------------------------===//
template <> duckdb::string Cast::Operation(bool input);
template <> duckdb::string Cast::Operation(int8_t input);
template <> duckdb::string Cast::Operation(int16_t input);
template <> duckdb::string Cast::Operation(int32_t input);
template <> duckdb::string Cast::Operation(int64_t input);
template <> duckdb::string Cast::Operation(uint64_t input);
template <> duckdb::string Cast::Operation(float input);
template <> duckdb::string Cast::Operation(double input);

struct CastFromDate {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
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
template <> duckdb::string CastFromDate::Operation(duckdb::date_t input);
template <> duckdb::date_t CastToDate::Operation(const char *input);
template <> duckdb::timestamp_t CastDateToTimestamp::Operation(duckdb::date_t input);

struct CastFromTime {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast from time could not be performed!");
	}
};
struct CastToTime {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to time could not be performed!");
	}
};
template <> duckdb::string CastFromTime::Operation(duckdb::dtime_t input);
template <> duckdb::dtime_t CastToTime::Operation(const char *input);

struct CastToTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC input) {
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
template <> duckdb::string CastFromTimestamp::Operation(duckdb::timestamp_t input);
template <> duckdb::timestamp_t CastToTimestamp::Operation(const char *input);

} // namespace duckdb
