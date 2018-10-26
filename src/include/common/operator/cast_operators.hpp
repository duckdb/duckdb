//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/cast_operators.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include <cstdint>

namespace operators {

struct Cast {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		return (DST)left;
	}
};

//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <> int8_t Cast::Operation(const char *left);
template <> int16_t Cast::Operation(const char *left);
template <> int Cast::Operation(const char *left);
template <> int64_t Cast::Operation(const char *left);
template <> uint64_t Cast::Operation(const char *left);
template <> double Cast::Operation(const char *left);
//===--------------------------------------------------------------------===//
// Numeric -> String Casts
//===--------------------------------------------------------------------===//
template <> std::string Cast::Operation(int8_t left);
template <> std::string Cast::Operation(int16_t left);
template <> std::string Cast::Operation(int left);
template <> std::string Cast::Operation(int64_t left);
template <> std::string Cast::Operation(uint64_t left);
template <> std::string Cast::Operation(double left);

struct CastFromDate {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		throw duckdb::NotImplementedException(
		    "Cast from date could not be performed!");
	}
};

struct CastToDate {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		throw duckdb::NotImplementedException(
		    "Cast to date could not be performed!");
	}
};

template <> int32_t CastFromDate::Operation(duckdb::date_t left);
template <> int64_t CastFromDate::Operation(duckdb::date_t left);
template <> std::string CastFromDate::Operation(duckdb::date_t left);
template <> duckdb::date_t CastToDate::Operation(const char *left);
template <> duckdb::date_t CastToDate::Operation(int32_t left);
template <> duckdb::date_t CastToDate::Operation(int64_t left);

} // namespace operators
