//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/decimal_cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
struct TryCastToDecimal {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
		throw NotImplementedException("Unimplemented type for TryCastToDecimal!");
	}
};

struct TryCastToDecimalCommaSeparated {
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

//===--------------------------------------------------------------------===//
// Cast Decimal <-> bool
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, bool &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, bool &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, bool &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bool &result, string *error_message, uint8_t width, uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int8_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int16_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int32_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int64_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> hugeint_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint8_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint8_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint8_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint8_t &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint16_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint32_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint64_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> float
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, float &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, float &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, float &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, float &result, string *error_message, uint8_t width, uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> double
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, double &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, double &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, double &result, string *error_message, uint8_t width, uint8_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, double &result, string *error_message, uint8_t width,
                                   uint8_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> VARCHAR
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, int16_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, int32_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, int64_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, hugeint_t &result, string *error_message, uint8_t width,
                                            uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, int16_t &result, string *error_message,
                                                          uint8_t width, uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, int32_t &result, string *error_message,
                                                          uint8_t width, uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, int64_t &result, string *error_message,
                                                          uint8_t width, uint8_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, hugeint_t &result, string *error_message,
                                                          uint8_t width, uint8_t scale);

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

} // namespace duckdb
