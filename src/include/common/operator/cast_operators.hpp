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

namespace duckdb {

struct Cast {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		return (DST)left;
	}
};

//===--------------------------------------------------------------------===//
// Numeric -> int8_t casts
//===--------------------------------------------------------------------===//
template <> int8_t Cast::Operation(int16_t left);
template <> int8_t Cast::Operation(int32_t left);
template <> int8_t Cast::Operation(int64_t left);
template <> int8_t Cast::Operation(uint64_t left);
template <> int8_t Cast::Operation(double left);
//===--------------------------------------------------------------------===//
// Numeric -> int16_t casts
//===--------------------------------------------------------------------===//
template <> int16_t Cast::Operation(int32_t left);
template <> int16_t Cast::Operation(int64_t left);
template <> int16_t Cast::Operation(uint64_t left);
template <> int16_t Cast::Operation(double left);
//===--------------------------------------------------------------------===//
// Numeric -> int32_t casts
//===--------------------------------------------------------------------===//
template <> int32_t Cast::Operation(int64_t left);
template <> int32_t Cast::Operation(uint64_t left);
template <> int32_t Cast::Operation(double left);
//===--------------------------------------------------------------------===//
// Numeric -> int64_t casts
//===--------------------------------------------------------------------===//
template <> int64_t Cast::Operation(uint64_t left);
template <> int64_t Cast::Operation(double left);
//===--------------------------------------------------------------------===//
// Numeric -> uint64_t casts
//===--------------------------------------------------------------------===//
template <> uint64_t Cast::Operation(int8_t left);
template <> uint64_t Cast::Operation(int16_t left);
template <> uint64_t Cast::Operation(int32_t left);
template <> uint64_t Cast::Operation(int64_t left);
template <> uint64_t Cast::Operation(double left);

//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <> bool Cast::Operation(const char *left);
template <> int8_t Cast::Operation(const char *left);
template <> int16_t Cast::Operation(const char *left);
template <> int32_t Cast::Operation(const char *left);
template <> int64_t Cast::Operation(const char *left);
template <> uint64_t Cast::Operation(const char *left);
template <> float Cast::Operation(const char *left);
template <> double Cast::Operation(const char *left);
//===--------------------------------------------------------------------===//
// Numeric -> String Casts
//===--------------------------------------------------------------------===//
template <> duckdb::string Cast::Operation(bool left);
template <> duckdb::string Cast::Operation(int8_t left);
template <> duckdb::string Cast::Operation(int16_t left);
template <> duckdb::string Cast::Operation(int32_t left);
template <> duckdb::string Cast::Operation(int64_t left);
template <> duckdb::string Cast::Operation(uint64_t left);
template <> duckdb::string Cast::Operation(float left);
template <> duckdb::string Cast::Operation(double left);

struct CastFromDate {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		throw duckdb::NotImplementedException("Cast from date could not be performed!");
	}
};

struct CastToDate {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		throw duckdb::NotImplementedException("Cast to date could not be performed!");
	}
};

template <> int32_t CastFromDate::Operation(duckdb::date_t left);
template <> int64_t CastFromDate::Operation(duckdb::date_t left);
template <> duckdb::string CastFromDate::Operation(duckdb::date_t left);
template <> duckdb::date_t CastToDate::Operation(const char *left);
template <> duckdb::date_t CastToDate::Operation(int32_t left);
template <> duckdb::date_t CastToDate::Operation(int64_t left);

struct CastToTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

struct CastFromTimestamp {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		throw duckdb::NotImplementedException("Cast to timestamp could not be performed!");
	}
};

template <> int64_t CastFromTimestamp::Operation(duckdb::timestamp_t left);
template <> duckdb::string CastFromTimestamp::Operation(duckdb::timestamp_t left);
template <> duckdb::timestamp_t CastToTimestamp::Operation(const char *left);
template <> duckdb::timestamp_t CastToTimestamp::Operation(int64_t left);

} // namespace duckdb
