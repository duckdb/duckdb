//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/operator/cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "common/exception.hpp"
#include "common/types/timestamp.hpp"
#include <iostream>

namespace duckdb {

struct Cast {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type=SQLType::SQLNULL) {
		return (DST)left;
	}
};

struct TryCast {
	template <class SRC, class DST> static inline bool Operation(SRC left, DST &target) {
		target = Cast::Operation(left);
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int16_t left, int8_t &result);
template <> bool TryCast::Operation(int32_t left, int8_t &result);
template <> bool TryCast::Operation(int64_t left, int8_t &result);
template <> bool TryCast::Operation(float left, int8_t &result);
template <> bool TryCast::Operation(double left, int8_t &result);

template <> int8_t Cast::Operation(int16_t left, SQLType target_type);
template <> int8_t Cast::Operation(int32_t left, SQLType target_type);
template <> int8_t Cast::Operation(int64_t left, SQLType target_type);
template <> int8_t Cast::Operation(float left, SQLType target_type);
template <> int8_t Cast::Operation(double left, SQLType target_type);
//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int32_t left, int16_t &result);
template <> bool TryCast::Operation(int64_t left, int16_t &result);
template <> bool TryCast::Operation(float left, int16_t &result);
template <> bool TryCast::Operation(double left, int16_t &result);

template <> int16_t Cast::Operation(int32_t left, SQLType target_type);
template <> int16_t Cast::Operation(int64_t left, SQLType target_type);
template <> int16_t Cast::Operation(float left, SQLType target_type);
template <> int16_t Cast::Operation(double left, SQLType target_type);
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(int64_t left, int32_t &result);
template <> bool TryCast::Operation(float left, int32_t &result);
template <> bool TryCast::Operation(double left, int32_t &result);

template <> int32_t Cast::Operation(int64_t left, SQLType target_type);
template <> int32_t Cast::Operation(float left, SQLType target_type);
template <> int32_t Cast::Operation(double left, SQLType target_type);
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(float left, int64_t &result);
template <> bool TryCast::Operation(double left, int64_t &result);

template <> int64_t Cast::Operation(float left, SQLType target_type);
template <> int64_t Cast::Operation(double left, SQLType target_type);
//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <> bool TryCast::Operation(const char *left, bool &result);
template <> bool TryCast::Operation(const char *left, int8_t &result);
template <> bool TryCast::Operation(const char *left, int16_t &result);
template <> bool TryCast::Operation(const char *left, int32_t &result);
template <> bool TryCast::Operation(const char *left, int64_t &result);
template <> bool TryCast::Operation(const char *left, float &result);
template <> bool TryCast::Operation(const char *left, double &result);

template <> bool Cast::Operation(const char *left, SQLType target_type);
template <> int8_t Cast::Operation(const char *left, SQLType target_type);
template <> int16_t Cast::Operation(const char *left, SQLType target_type);
template <> int32_t Cast::Operation(const char *left, SQLType target_type);
template <> int64_t Cast::Operation(const char *left, SQLType target_type);
template <> float Cast::Operation(const char *left, SQLType target_type);
template <> double Cast::Operation(const char *left, SQLType target_type);
//===--------------------------------------------------------------------===//
// Numeric -> String Casts
//===--------------------------------------------------------------------===//
template <> duckdb::string Cast::Operation(bool left, SQLType target_type);
template <> duckdb::string Cast::Operation(int8_t left, SQLType target_type);
template <> duckdb::string Cast::Operation(int16_t left, SQLType target_type);
template <> duckdb::string Cast::Operation(int32_t left, SQLType target_type);
template <> duckdb::string Cast::Operation(int64_t left, SQLType target_type);
template <> duckdb::string Cast::Operation(uint64_t left, SQLType target_type);
template <> duckdb::string Cast::Operation(float left, SQLType target_type);
template <> duckdb::string Cast::Operation(double left, SQLType target_type);

struct CastFromDate {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
		throw duckdb::NotImplementedException("Cast from date could not be performed!");
	}
};

struct CastToDate {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
		throw duckdb::NotImplementedException("Cast to date could not be performed!");
	}
};

template <> int32_t CastFromDate::Operation(duckdb::date_t left, SQLType target_type);
template <> int64_t CastFromDate::Operation(duckdb::date_t left, SQLType target_type);
template <> duckdb::string CastFromDate::Operation(duckdb::date_t left, SQLType target_type);
template <> duckdb::date_t CastToDate::Operation(const char *left, SQLType target_type);
template <> duckdb::date_t CastToDate::Operation(int32_t left, SQLType target_type);
template <> duckdb::date_t CastToDate::Operation(int64_t left, SQLType target_type);

struct CastFromTime {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
		throw duckdb::NotImplementedException("Cast from time could not be performed!");
	}
};
struct CastToTime {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
		throw duckdb::NotImplementedException("Cast to time could not be performed!");
	}
};
template <> int32_t CastFromTime::Operation(duckdb::dtime_t left, SQLType target_type);
template <> int64_t CastFromTime::Operation(duckdb::dtime_t left, SQLType target_type);
template <> duckdb::string CastFromTime::Operation(duckdb::dtime_t left, SQLType target_type);
template <> duckdb::dtime_t CastToTime::Operation(const char *left, SQLType target_type);
template <> duckdb::dtime_t CastToTime::Operation(int32_t left, SQLType target_type);
template <> duckdb::dtime_t CastToTime::Operation(int64_t left, SQLType target_type);

struct CastToTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

template <> int64_t CastFromTimestamp::Operation(duckdb::timestamp_t left, SQLType target_type);
template <> duckdb::string CastFromTimestamp::Operation(duckdb::timestamp_t left, SQLType target_type);
template <> duckdb::timestamp_t CastToTimestamp::Operation(const char *left, SQLType target_type);
template <> duckdb::timestamp_t CastToTimestamp::Operation(int64_t left, SQLType target_type);


struct CastToInterval {
    template <class SRC, class DST> static inline DST Operation(SRC left, SQLType target_type) {
        std::cout << " tpppp >>> " << typeid(left).name() << std::endl;
        throw duckdb::NotImplementedException(string("Cast to interval could not be performed! ")+ typeid(left).name());
    }
};

template <> duckdb::Interval CastToInterval::Operation(const char *left, SQLType target_type);

} // namespace duckdb
