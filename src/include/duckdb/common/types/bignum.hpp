//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/bignum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/bignum.hpp"

namespace duckdb {
using digit_t = uint32_t;
using twodigit_t = uint64_t;

//! The Bignum class is a static class that holds helper functions for the Bignum type.
class Bignum {
public:
	//! This is the maximum number of bytes a BIGNUM can have on it's data size
	//! i.e., 2^(8*3-1) - 1.
	DUCKDB_API static constexpr uint32_t MAX_DATA_SIZE = 8388607;
	//! Header size of a Bignum is always 3 bytes.
	DUCKDB_API static constexpr uint8_t BIGNUM_HEADER_SIZE = 3;
	//! Max(e such that 10**e fits in a digit_t)
	DUCKDB_API static constexpr uint8_t DECIMAL_SHIFT = 9;
	//! 10 ** DECIMAL_SHIFT
	DUCKDB_API static constexpr digit_t DECIMAL_BASE = 1000000000;
	//! Bytes of a digit_t
	DUCKDB_API static constexpr uint8_t DIGIT_BYTES = sizeof(digit_t);
	//! Bits of a digit_t
	DUCKDB_API static constexpr uint8_t DIGIT_BITS = DIGIT_BYTES * 8;
	//! Verifies if a Bignum is valid. i.e., if it has 3 header bytes. The header correctly represents the number of
	//! data bytes, and the data bytes has no leading zero bytes.
	DUCKDB_API static void Verify(const bignum_t &input);

	//! Sets the header of a bignum (i.e., char* blob), depending on the number of bytes that bignum needs and if it's a
	//! negative number
	DUCKDB_API static void SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative);
	//! Initializes and returns a blob with value 0, allocated in Vector& result
	DUCKDB_API static bignum_t InitializeBignumZero(Vector &result);
	DUCKDB_API static string InitializeBignumZero();

	//! Switch Case of To Bignum Convertion
	DUCKDB_API static BoundCastInfo NumericToBignumCastSwitch(const LogicalType &source);

	//! ----------------------------------- Varchar Cast ----------------------------------- //
	//! Function to prepare a varchar for conversion. We trim zero's, check for negative values, and what-not
	//! Returns false if this is an invalid varchar
	DUCKDB_API static bool VarcharFormatting(const string_t &value, idx_t &start_pos, idx_t &end_pos, bool &is_negative,
	                                         bool &is_zero);

	//! Converts a char to a Digit
	DUCKDB_API static int CharToDigit(char c);
	//! Converts a Digit to a char
	DUCKDB_API static char DigitToChar(int digit);
	//! Function to convert a string_t into a vector of bytes
	DUCKDB_API static void GetByteArray(vector<uint8_t> &byte_array, bool &is_negative, const string_t &blob);
	//! Function to create a BIGNUM blob from a byte array containing the absolute value, plus an is_negative bool
	DUCKDB_API static string FromByteArray(uint8_t *data, idx_t size, bool is_negative);
	//! Function to convert BIGNUM blob to a VARCHAR
	DUCKDB_API static string BignumToVarchar(const bignum_t &blob);
	//! Function to convert Varchar to BIGNUM blob
	DUCKDB_API static string VarcharToBignum(const string_t &value);
	//! ----------------------------------- Double Cast ----------------------------------- //
	DUCKDB_API static bool BignumToDouble(const bignum_t &blob, double &result, bool &strict);
	template <class T>
	static bool BignumToInt(const bignum_t &blob, T &result, bool &strict) {
		auto data_byte_size = blob.data.GetSize() - BIGNUM_HEADER_SIZE;
		auto data = blob.data.GetData();
		bool is_negative = (data[0] & 0x80) == 0;

		uhugeint_t abs_value = 0;
		for (idx_t i = 0; i < data_byte_size; ++i) {
			uint8_t byte = static_cast<uint8_t>(data[Bignum::BIGNUM_HEADER_SIZE + i]);
			if (is_negative) {
				byte = ~byte;
			}
			abs_value = (abs_value << 8) | byte;
		}

		if (is_negative) {
			if (abs_value > static_cast<uhugeint_t>(std::numeric_limits<T>::max()) + 1) {
				throw OutOfRangeException("Negative bignum too small for type");
			}
			result = static_cast<T>(-static_cast<hugeint_t>(abs_value));
		} else {
			if (abs_value > static_cast<uhugeint_t>(std::numeric_limits<T>::max())) {
				throw OutOfRangeException("Positive bignum too large for type");
			}
			result = static_cast<T>(abs_value);
		}
		return true;
	}
};

//! ----------------------------------- (u)Integral Cast ----------------------------------- //
struct IntCastToBignum {
	template <class SRC>
	static inline bignum_t Operation(SRC input, Vector &result) {
		return IntToBignum(result, input);
	}
};

//! ----------------------------------- (u)HugeInt Cast ----------------------------------- //
struct HugeintCastToBignum {
	template <class SRC>
	static inline bignum_t Operation(SRC input, Vector &result) {
		throw InternalException("Unsupported type for cast to BIGNUM");
	}
};

struct TryCastToBignum {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		throw InternalException("Unsupported type for try cast to BIGNUM");
	}
};

template <>
DUCKDB_API bool TryCastToBignum::Operation(double double_value, bignum_t &result_value, Vector &result,
                                           CastParameters &parameters);

template <>
DUCKDB_API bool TryCastToBignum::Operation(float float_value, bignum_t &result_value, Vector &result,
                                           CastParameters &parameters);

template <>
DUCKDB_API bool TryCastToBignum::Operation(string_t input_value, bignum_t &result_value, Vector &result,
                                           CastParameters &parameters);

struct BignumCastToVarchar {
	template <class SRC>
	DUCKDB_API static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, Bignum::BignumToVarchar(input));
	}
};

struct BignumToDoubleCast {
	template <class SRC, class DST>
	DUCKDB_API static inline bool Operation(SRC input, DST &result, bool strict = false) {
		return Bignum::BignumToDouble(input, result, strict);
	}
};

struct BignumToIntCast {
	template <class SRC, class DST>
	DUCKDB_API static inline bool Operation(SRC input, DST &result, bool strict = false) {
		return Bignum::BignumToInt(input, result, strict);
	}
};

} // namespace duckdb
