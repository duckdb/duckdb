//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/varint.hpp
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

namespace duckdb {
//! The Varint class is a static class that holds helper functions for the Varint type.
class Varint {
public:
	//! Header size of a Varint is always 3 bytes.
	DUCKDB_API static constexpr uint8_t VARINT_HEADER_SIZE = 3;
	//! Verifies if a Varint is valid. i.e., if it has 3 header bytes. The header correctly represents the number of
	//! data bytes, and the data bytes has no leading zero bytes.
	DUCKDB_API static void Verify(const string_t &input);

	//! Sets the header of a varint (i.e., char* blob), depending on the number of bytes that varint needs and if it's a
	//! negative number
	DUCKDB_API static void SetHeader(char *blob, uint64_t number_of_bytes, bool is_negative);
	//! Initializes and returns a blob with value 0, allocated in Vector& result
	DUCKDB_API static string_t InitializeVarintZero(Vector &result);
	DUCKDB_API static string InitializeVarintZero();

	//! Switch Case of To Varint Convertion
	DUCKDB_API static BoundCastInfo NumericToVarintCastSwitch(const LogicalType &source);

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
	//! Function to convert VARINT blob to a VARCHAR
	DUCKDB_API static string VarIntToVarchar(const string_t &blob);
	//! Function to convert Varchar to VARINT blob
	DUCKDB_API static string VarcharToVarInt(const string_t &value);
	//! ----------------------------------- Double Cast ----------------------------------- //
	DUCKDB_API static bool VarintToDouble(const string_t &blob, double &result, bool &strict);
};

//! ----------------------------------- (u)Integral Cast ----------------------------------- //
struct IntCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return IntToVarInt(result, input);
	}
};

//! ----------------------------------- (u)HugeInt Cast ----------------------------------- //
struct HugeintCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		throw InternalException("Unsupported type for cast to VARINT");
	}
};

struct TryCastToVarInt {
	template <class SRC, class DST>
	static inline bool Operation(SRC input, DST &result, Vector &result_vector, CastParameters &parameters) {
		throw InternalException("Unsupported type for try cast to VARINT");
	}
};

template <>
DUCKDB_API bool TryCastToVarInt::Operation(double double_value, string_t &result_value, Vector &result,
                                           CastParameters &parameters);

template <>
DUCKDB_API bool TryCastToVarInt::Operation(float float_value, string_t &result_value, Vector &result,
                                           CastParameters &parameters);

template <>
DUCKDB_API bool TryCastToVarInt::Operation(string_t input_value, string_t &result_value, Vector &result,
                                           CastParameters &parameters);

struct VarIntCastToVarchar {
	template <class SRC>
	DUCKDB_API static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, Varint::VarIntToVarchar(input));
	}
};

struct VarintToDoubleCast {
	template <class SRC, class DST>
	DUCKDB_API static inline bool Operation(SRC input, DST &result, bool strict = false) {
		return Varint::VarintToDouble(input, result, strict);
	}
};

} // namespace duckdb
