//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/types/operators.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <string>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/types/date.hpp"
#include "common/types/hash.hpp"
#include "common/types/null_value.hpp"

namespace operators {

//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
struct Not {
	static inline bool Operation(bool left) {
		return !left;
	}
};

//===--------------------------------------------------------------------===//
// Aggregation Operations
//===--------------------------------------------------------------------===//
struct Max {
	template <class T> static inline T Operation(T left, T right) {
		return std::max(left, right);
	}
};

struct Min {
	template <class T> static inline T Operation(T left, T right) {
		return std::min(left, right);
	}
};

struct PickLeft {
	template <class T> static inline T Operation(T left, T right) {
		return left;
	}
};

struct PickRight {
	template <class T> static inline T Operation(T left, T right) {
		return right;
	}
};

struct ConstantZero {
	template <class T> static inline T Operation(T left, T right) {
		return 0;
	}
};

struct ConstantOne {
	template <class T> static inline T Operation(T left, T right) {
		return 1;
	}
};

struct AddOne {
	template <class T> static inline T Operation(T left, T right) {
		return right + 1;
	}
};

struct AnyTrue {
	static inline uint8_t Operation(uint8_t val, uint8_t result) {
		return result || val == true;
	}
};

struct AllTrue {
	static inline uint8_t Operation(uint8_t val, uint8_t result) {
		return result && val == true;
	}
};

struct MaximumStringLength {
	static inline uint64_t Operation(const char *str, uint64_t right) {
		return std::max((uint64_t)strlen(str), right);
	}
};

//===--------------------------------------------------------------------===//
// Casts
//===--------------------------------------------------------------------===//
struct Cast {
	template <class SRC, class DST> static inline DST Operation(SRC left) {
		return (DST)left;
	}
};

// string casts
// string -> numeric
template <> int8_t Cast::Operation(const char *left);
template <> int16_t Cast::Operation(const char *left);
template <> int Cast::Operation(const char *left);
template <> int64_t Cast::Operation(const char *left);
template <> uint64_t Cast::Operation(const char *left);
template <> double Cast::Operation(const char *left);
// numeric -> std::string
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

struct NOP {
	template <class T> static inline T Operation(T left) {
		return left;
	}
};

} // namespace operators
