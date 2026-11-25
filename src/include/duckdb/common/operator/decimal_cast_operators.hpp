//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/decimal_cast_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bignum.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/integer_cast_operator.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
//! Decimals are stored in four fixed-size integer tiers (int16_t, int32_t, int64_t, hugeint_t) and, above 38 digits,
//! as a bignum_t: that tier allocates its results in the result vector, so its operations take the vector as well
struct TryCastToDecimal {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, CastParameters &parameters, uint32_t width, uint32_t scale) {
		NotImplementedException::Throw("Unimplemented type for TryCastToDecimal!");
	}
	template <class SRC>
	static inline bool Operation(SRC input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
	                             uint32_t width, uint32_t scale) {
		NotImplementedException::Throw("Unimplemented type for TryCastToDecimal!");
	}
};

struct TryCastToDecimalCommaSeparated {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, CastParameters &parameters, uint32_t width, uint32_t scale) {
		NotImplementedException::Throw("Unimplemented type for TryCastToDecimal!");
	}
	template <class SRC>
	static inline bool Operation(SRC input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
	                             uint32_t width, uint32_t scale) {
		NotImplementedException::Throw("Unimplemented type for TryCastToDecimal!");
	}
};

struct TryCastFromDecimal {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, CastParameters &parameters, uint32_t width, uint32_t scale) {
		NotImplementedException::Throw("Unimplemented type for TryCastFromDecimal!");
	}
	template <class SRC>
	static inline bool Operation(SRC input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
	                             uint32_t width, uint32_t scale) {
		NotImplementedException::Throw("Unimplemented type for TryCastFromDecimal!");
	}
};

//! Runs a decimal cast operation whose result is stored in a vector: the fixed-size tiers hold the result in place,
//! the bignum_t tier allocates it in the result vector
struct VectorDecimalCast {
	template <class OP, class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters,
	                             uint32_t width, uint32_t scale) {
		if constexpr (std::is_same<DST, bignum_t>::value) {
			return OP::template Operation<SRC>(input, result, result_vector, parameters, width, scale);
		} else {
			return OP::template Operation<SRC, DST>(input, result, parameters, width, scale);
		}
	}
};

//===--------------------------------------------------------------------===//
// Cast Decimal <-> bool
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bool input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, bool &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, bool &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, bool &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bool &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, bool &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int8_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int8_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, int8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int16_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int16_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int32_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int32_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> int64_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(int64_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> hugeint_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, int16_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, int32_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, int64_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(hugeint_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uhugeint_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uhugeint_t input, int16_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uhugeint_t input, int32_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uhugeint_t input, int64_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uhugeint_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uhugeint_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uhugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uhugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uhugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uhugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, uhugeint_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint8_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint8_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, uint8_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint16_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint16_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, uint16_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint32_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint32_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, uint32_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> uint64_t
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(uint64_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, uint64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, uint64_t &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> float
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(float input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, float &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, float &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, float &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, float &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, float &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> double
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, hugeint_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(double input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, double &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, double &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, double &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, double &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, double &result, CastParameters &parameters, uint32_t width,
                                   uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> VARCHAR
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(string_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, int16_t &result, CastParameters &parameters,
                                                          uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, int32_t &result, CastParameters &parameters,
                                                          uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, int64_t &result, CastParameters &parameters,
                                                          uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, hugeint_t &result, CastParameters &parameters,
                                                          uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimalCommaSeparated::Operation(string_t input, bignum_t &result, Vector &result_vector,
                                                          CastParameters &parameters, uint32_t width, uint32_t scale);

//===--------------------------------------------------------------------===//
// Cast Decimal <-> bignum_t (BIGNUM, a decimal with scale 0)
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bignum_t input, int16_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bignum_t input, int32_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bignum_t input, int64_t &result, CastParameters &parameters, uint32_t width,
                                            uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bignum_t input, hugeint_t &result, CastParameters &parameters,
                                            uint32_t width, uint32_t scale);
template <>
DUCKDB_API bool TryCastToDecimal::Operation(bignum_t input, bignum_t &result, Vector &result_vector,
                                            CastParameters &parameters, uint32_t width, uint32_t scale);

template <>
bool TryCastFromDecimal::Operation(int16_t input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
                                   uint32_t width, uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int32_t input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
                                   uint32_t width, uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(int64_t input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
                                   uint32_t width, uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
                                   uint32_t width, uint32_t scale);
template <>
bool TryCastFromDecimal::Operation(bignum_t input, bignum_t &result, Vector &result_vector, CastParameters &parameters,
                                   uint32_t width, uint32_t scale);

struct StringCastFromDecimal {
	template <class SRC>
	static inline string_t Operation(SRC input, uint32_t width, uint32_t scale, StringHeap &heap) {
		NotImplementedException::Throw("Unimplemented type for string cast!");
	}
};

template <>
string_t StringCastFromDecimal::Operation(int16_t input, uint32_t width, uint32_t scale, StringHeap &heap);
template <>
string_t StringCastFromDecimal::Operation(int32_t input, uint32_t width, uint32_t scale, StringHeap &heap);
template <>
string_t StringCastFromDecimal::Operation(int64_t input, uint32_t width, uint32_t scale, StringHeap &heap);
template <>
string_t StringCastFromDecimal::Operation(hugeint_t input, uint32_t width, uint32_t scale, StringHeap &heap);
template <>
string_t StringCastFromDecimal::Operation(bignum_t input, uint32_t width, uint32_t scale, StringHeap &heap);

//===--------------------------------------------------------------------===//
// Cast VARCHAR <-> Decimal
//===--------------------------------------------------------------------===//
enum class ExponentType : uint8_t { NONE, POSITIVE, NEGATIVE };

template <typename T>
struct DecimalCastTraits {
	using POWERS_OF_TEN_CLASS = NumericHelper;
};

template <>
struct DecimalCastTraits<hugeint_t> {
	using POWERS_OF_TEN_CLASS = Hugeint;
};

template <>
struct DecimalCastTraits<uhugeint_t> {
	using POWERS_OF_TEN_CLASS = Uhugeint;
};

template <class T>
struct DecimalCastData {
	using StoreType = T;
	StoreType result;     // holds the digits as we collect them
	uint32_t width;       // the width required in the result
	uint32_t scale;       // the scale required in the result
	uint32_t digit_count; // total number of digits collected so far
	int32_t digit_scale;  // scale of the recorded digits.
	                      // negative if we've skipped some digits
	                      // positive if we've recorded some decimals
	bool round_set;       // whether we've recorded a rounding digit ...
	bool round_up;        // ... and was the rounding digit >= 5
	StoreType limit;
};

//! StoreType max/min divided by 10, for the overflow check in DecimalCastOperation::HandleDigit.
//! These are forced to be compile-time constants so the division is folded away: with a hugeint_t
//! store type it is a 128-bit division that would otherwise run for every digit. hugeint_t::operator/
//! is constexpr, but it is too large to be inlined at the call site, so the compiler does not fold it
//! unless constant evaluation is actually required.
template <class T>
struct DecimalCastLimits {
	static constexpr T MaximumDiv10 = NumericLimits<T>::Maximum() / 10;
	static constexpr T MinimumDiv10 = NumericLimits<T>::Minimum() / 10;
};

struct DecimalCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		if (state.result == 0 && digit == 0) {
			// leading zero's don't count towards the digit count
			return true;
		}
		if (state.digit_count < state.width) {
			// record digits up to the maximum width
			state.digit_count++;
			return AppendDigit<T, NEGATIVE>(state, digit);
		} else {
			// when we reach maximum width, record rounding
			// and ignore all subsequent digits.
			if (!state.round_set) {
				state.round_set = true;
				state.round_up = digit >= 5;
			}
			state.digit_scale--;
			return true;
		}
	}

	template <class T, bool NEGATIVE>
	static bool AppendDigit(T &state, uint8_t digit) {
		if (NEGATIVE) {
			if (state.result < DecimalCastLimits<typename T::StoreType>::MinimumDiv10) {
				return false;
			}
			state.result = state.result * 10 - digit;
		} else {
			if (state.result > DecimalCastLimits<typename T::StoreType>::MaximumDiv10) {
				return false;
			}
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static void RoundUpResult(T &state) {
		if (NEGATIVE) {
			state.result -= 1;
		} else {
			state.result += 1;
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		// note this is called *instead of* Finalize if
		// there's an exponent part.
		state.digit_scale -= exponent;
		return Finalize<T, NEGATIVE>(state);
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.result == 0 && digit == 0) {
			// if this is a leading zero decimal point, don't
			// record it just increase the count of decimals.
			state.digit_scale++;
			return true;
		}
		if (state.digit_count < state.width) {
			// keep recording digits up to the maximum width.
			state.digit_scale++;
			state.digit_count++;
			return AppendDigit<T, NEGATIVE>(state, digit);
		} else {
			// if we've reached the maximum width, record
			// if we need to round up the last digit and
			// ignore all subsequent digits.
			if (!state.round_set) {
				state.round_set = true;
				state.round_up = digit >= 5;
			}
			return true;
		}
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		int32_t signed_state_scale = UnsafeNumericCast<int32_t>(state.scale);
		for (int32_t i = 0; i < state.digit_scale - signed_state_scale; i++) {
			// divide by tens to get rid of excessive decimals, updating
			// the rounding flag as we chop digits off
			auto mod = state.result % 10;
			state.round_up = NEGATIVE ? mod <= -5 : mod >= 5;
			state.result /= static_cast<typename T::StoreType>(10.0);
			if (state.result == 0)
				break;
		}
		// if the first digit past our width cutoff was >= 5, we need
		// to round up our least digit.
		if (state.round_up) {
			RoundUpResult<T, NEGATIVE>(state);
		}
		for (int32_t i = 0; i < signed_state_scale - state.digit_scale; i++) {
			// if our current scale is too small, add zeros to pad
			// the scale out by adding zeros.  Note that either the
			// previous loop or this loop run, never both.
			if (!AppendDigit<T, NEGATIVE>(state, 0))
				return false;
		}
		return WithinLimit<T, NEGATIVE>(state);
	}

	template <class T, bool NEGATIVE>
	static bool WithinLimit(T &state) {
		if (NEGATIVE) {
			return state.result > -state.limit;
		} else {
			return state.result < state.limit;
		}
	}
};

template <class T, char decimal_separator = '.'>
bool TryDecimalStringCast(string_t input, T &result, CastParameters &parameters, uint32_t width, uint32_t scale) {
	return TryDecimalStringCast<T, decimal_separator>(input.GetData(), input.GetSize(), result, parameters, width,
	                                                  scale);
}

template <class T, char decimal_separator = '.'>
bool TryDecimalStringCast(const char *string_ptr, idx_t string_size, T &result, CastParameters &parameters,
                          uint32_t width, uint32_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.digit_scale = 0;
	state.round_set = false;
	state.round_up = false;
	state.limit = UnsafeNumericCast<T>(DecimalCastTraits<T>::POWERS_OF_TEN_CLASS::POWERS_OF_TEN[width]);
	if (!TryIntegerCast<DecimalCastData<T>, true, true, DecimalCastOperation, false, decimal_separator>(
	        string_ptr, string_size, state, false)) {
		string_t value(string_ptr, (uint32_t)string_size);
		string error = StringUtil::Format("Could not convert string \"%s\" to DECIMAL(%d,%d)", value.GetString(),
		                                  (int)width, (int)scale);
		HandleCastError::AssignError(error, parameters);
		return false;
	}
	result = state.result;
	return true;
}

template <class T, char decimal_separator = '.'>
bool TryDecimalStringCast(const char *string_ptr, idx_t string_size, T &result, uint32_t width, uint32_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.digit_scale = 0;
	state.round_set = false;
	state.round_up = false;
	state.limit = UnsafeNumericCast<T>(DecimalCastTraits<T>::POWERS_OF_TEN_CLASS::POWERS_OF_TEN[width]);
	if (!TryIntegerCast<DecimalCastData<T>, true, true, DecimalCastOperation, false, decimal_separator>(
	        string_ptr, string_size, state, false)) {
		return false;
	}
	result = state.result;
	return true;
}

} // namespace duckdb
