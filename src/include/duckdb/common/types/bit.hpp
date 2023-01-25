//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/bit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! The Bit class is a static class that holds helper functions for the BIT type.
class Bit {
public:
	//! Returns the number of bits in the bit string
	DUCKDB_API static idx_t BitLength(string_t bits);
	//! Returns the number of set bits in the bit string
	DUCKDB_API static idx_t BitCount(string_t bits);
	//! Returns the number of bytes in the bit string
	DUCKDB_API static idx_t OctetLength(string_t bits);
	//! Converts bits to a string, writing the output to the designated output string.
	//! The string needs to have space for at least GetStringSize(bits) bytes.
	DUCKDB_API static void ToString(string_t bits, char *output);
	//! Returns the bit size of a string -> bit conversion
	DUCKDB_API static bool TryGetBitStringSize(string_t str, idx_t &result_size, string *error_message);
	//! Convert a string to a bit. This function should ONLY be called after calling GetBitSize, since it does NOT
	//! perform data validation.
	DUCKDB_API static void ToBit(string_t str, data_ptr_t output);
	//! Extracts the nth bit from bit string; the first (leftmost) bit is indexed 0
	DUCKDB_API static idx_t GetBit(string_t bit_string, idx_t n);
	//! Sets the nth bit in bit string to newvalue; the first (leftmost) bit is indexed 0
	DUCKDB_API static void SetBit(string_t &bit_string, idx_t n, idx_t new_value);
	//! Returns first starting index of the specified substring within bits, or zero if it's not present.
	DUCKDB_API static idx_t BitPosition(string_t substring, string_t bits);

private:
	//! Returns the amount of padded zeroes to fill up to a full byte. This information is stored in the first byte of
	//! the bitstring.
	DUCKDB_API static idx_t GetPadding(string_t &bit_string);
};
} // namespace duckdb
