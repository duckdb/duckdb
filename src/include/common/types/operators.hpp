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

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/types/date.hpp"
#include "common/types/hash.hpp"

namespace operators {

struct ExecuteIgnoreNull {
	template <class L, class R, class RES, class OP>
	static inline RES Operation(L left, R right) {
		if (duckdb::IsNullValue<L>(left)) {
			return right;
		}
		if (duckdb::IsNullValue<R>(right)) {
			return left;
		}
		return OP::Operation(left, right);
	}
};
struct ExecuteIgnoreLeftNull {
	template <class L, class R, class RES, class OP>
	static inline RES Operation(L left, R right) {
		if (duckdb::IsNullValue<L>(left)) {
			return right;
		}
		return OP::Operation(left, right);
	}
};
struct ExecuteWithNullHandling {
	template <class L, class R, class RES, class OP>
	static inline RES Operation(L left, R right) {
		if (duckdb::IsNullValue<L>(left) || duckdb::IsNullValue<R>(right)) {
			return duckdb::NullValue<RES>();
		}
		return OP::Operation(left, right);
	}
};
struct ExecuteWithoutNullHandling {
	template <class L, class R, class RES, class OP>
	static inline RES Operation(L left, R right) {
		return OP::Operation(left, right);
	}
};
//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
struct Addition {
	template <class T> static inline T Operation(T left, T right) {
		return left + right;
	}
};

struct Subtraction {
	template <class T> static inline T Operation(T left, T right) {
		return left - right;
	}
};

struct Multiplication {
	template <class T> static inline T Operation(T left, T right) {
		return left * right;
	}
};

struct Division {
	template <class T> static inline T Operation(T left, T right) {
		if (right == 0) {
			return duckdb::NullValue<T>();
		}
		return left / right;
	}
};

struct Modulo {
	template <class T> static inline T Operation(T left, T right) {
		return left % right;
	}
};

template <> double Modulo::Operation(double left, double right);

struct XOR {
	template <class T> static inline T Operation(T left, T right) {
		return left ^ right;
	}
};

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
struct Equals {
	template <class T> static inline bool Operation(T left, T right) {
		return left == right;
	}
};

struct NotEquals {
	template <class T> static inline bool Operation(T left, T right) {
		return left != right;
	}
};

struct GreaterThan {
	template <class T> static inline bool Operation(T left, T right) {
		return left > right;
	}
};

struct GreaterThanEquals {
	template <class T> static inline bool Operation(T left, T right) {
		return left >= right;
	}
};

struct LessThan {
	template <class T> static inline bool Operation(T left, T right) {
		return left < right;
	}
};

struct LessThanEquals {
	template <class T> static inline bool Operation(T left, T right) {
		return left <= right;
	}
};

struct And {
	static inline bool Operation(bool left, bool right) {
		return left && right;
	}
};
struct Or {
	static inline bool Operation(bool left, bool right) {
		return left || right;
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

struct SetCount {
	template <class T> static inline T Operation(T left, T right) {
		return duckdb::IsNullValue(left) ? 0 : 1;
	}
};

struct AddOne {
	template <class T> static inline T Operation(T left, T right) {
		return right + 1;
	}
};

struct Hash {
	template <class T> static inline int32_t Operation(T left) {
		return duckdb::Hash<T>(left);
	}
};

struct NullCheck {
	template <class T> static inline bool Operation(T left, bool right) {
		return duckdb::IsNullValue<T>(left) || right;
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
// numeric -> string
template <> const char *Cast::Operation(int8_t left);
template <> const char *Cast::Operation(int16_t left);
template <> const char *Cast::Operation(int left);
template <> const char *Cast::Operation(int64_t left);
template <> const char *Cast::Operation(uint64_t left);
template <> const char *Cast::Operation(double left);

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

template <> char *CastFromDate::Operation(duckdb::date_t left);
template <> duckdb::date_t CastToDate::Operation(const char *left);

struct NOP {
	template <class T> static inline T Operation(T left) { return left; }
};

} // namespace operators
