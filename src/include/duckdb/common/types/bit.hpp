//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/bit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

using bitstring_t = duckdb::string_t;

//! The Bit class is a static class that holds helper functions for the BIT type.
class Bit {
public:
	//! Returns the number of bits in the bit string
	DUCKDB_API static idx_t BitLength(bitstring_t bits);
	//! Returns the number of set bits in the bit string
	DUCKDB_API static idx_t BitCount(bitstring_t bits);
	//! Returns the number of bytes in the bit string
	DUCKDB_API static idx_t OctetLength(bitstring_t bits);
	//! Extracts the nth bit from bit string; the first (leftmost) bit is indexed 0
	DUCKDB_API static idx_t GetBit(bitstring_t bit_string, idx_t n);
	//! Sets the nth bit in bit string to newvalue; the first (leftmost) bit is indexed 0
	DUCKDB_API static void SetBit(bitstring_t &bit_string, idx_t n, idx_t new_value);
	//! Returns first starting index of the specified substring within bits, or zero if it's not present.
	DUCKDB_API static idx_t BitPosition(bitstring_t substring, bitstring_t bits);
	//! Converts bits to a string, writing the output to the designated output string.
	//! The string needs to have space for at least GetStringSize(bits) bytes.
	DUCKDB_API static void ToString(bitstring_t bits, char *output);
	DUCKDB_API static string ToString(bitstring_t bits);
	//! Returns the bit size of a string -> bit conversion
	DUCKDB_API static bool TryGetBitStringSize(string_t str, idx_t &result_size, string *error_message);
	//! Convert a string to a bit. This function should ONLY be called after calling GetBitSize, since it does NOT
	//! perform data validation.
	DUCKDB_API static void ToBit(string_t str, bitstring_t &output);

	DUCKDB_API static string ToBit(string_t str);

	//! output needs to have enough space allocated before calling this function (blob size + 1)
	DUCKDB_API static void BlobToBit(string_t blob, bitstring_t &output);

	DUCKDB_API static string BlobToBit(string_t blob);

	//! output_str needs to have enough space allocated before calling this function (sizeof(T) + 1)
	template <class T>
	static void NumericToBit(T numeric, bitstring_t &output_str);

	template <class T>
	static string NumericToBit(T numeric);

	//! bit is expected to fit inside of output num (bit size <= sizeof(T) + 1)
	template <class T>
	static void BitToNumeric(bitstring_t bit, T &output_num);

	template <class T>
	static T BitToNumeric(bitstring_t bit);

	//! bit is expected to fit inside of output_blob (bit size = output_blob + 1)
	static void BitToBlob(bitstring_t bit, string_t &output_blob);

	static string BitToBlob(bitstring_t bit);

	//! Creates a new bitstring of determined length
	DUCKDB_API static void BitString(const string_t &input, idx_t len, bitstring_t &result);
	DUCKDB_API static void ExtendBitString(const bitstring_t &input, idx_t bit_length, bitstring_t &result);
	DUCKDB_API static void SetEmptyBitString(bitstring_t &target, string_t &input);
	DUCKDB_API static void SetEmptyBitString(bitstring_t &target, idx_t len);
	DUCKDB_API static idx_t ComputeBitstringLen(idx_t len);

	DUCKDB_API static void RightShift(const bitstring_t &bit_string, idx_t shift, bitstring_t &result);
	DUCKDB_API static void LeftShift(const bitstring_t &bit_string, idx_t shift, bitstring_t &result);
	DUCKDB_API static void BitwiseAnd(const bitstring_t &rhs, const bitstring_t &lhs, bitstring_t &result);
	DUCKDB_API static void BitwiseOr(const bitstring_t &rhs, const bitstring_t &lhs, bitstring_t &result);
	DUCKDB_API static void BitwiseXor(const bitstring_t &rhs, const bitstring_t &lhs, bitstring_t &result);
	DUCKDB_API static void BitwiseNot(const bitstring_t &rhs, bitstring_t &result);

	DUCKDB_API static void Verify(const bitstring_t &input);

private:
	static void Finalize(bitstring_t &str);
	static idx_t GetBitInternal(bitstring_t bit_string, idx_t n);
	static void SetBitInternal(bitstring_t &bit_string, idx_t n, idx_t new_value);
	static idx_t GetBitIndex(idx_t n);
	static uint8_t GetFirstByte(const bitstring_t &str);
};

//===--------------------------------------------------------------------===//
// Bit Template definitions
//===--------------------------------------------------------------------===//
template <class T>
void Bit::NumericToBit(T numeric, bitstring_t &output_str) {
	D_ASSERT(output_str.GetSize() >= sizeof(T) + 1);

	auto le_numeric = BSwapIfBE(numeric);
	auto output = output_str.GetDataWriteable();
	auto data = const_data_ptr_cast(&le_numeric);

	*output = 0; // set padding to 0
	++output;
	for (idx_t idx = 0; idx < sizeof(T); ++idx) {
		output[idx] = static_cast<char>(data[sizeof(T) - idx - 1]);
	}
	Bit::Finalize(output_str);
}

template <class T>
string Bit::NumericToBit(T numeric) {
	auto bit_len = sizeof(T) + 1;
	auto buffer = make_unsafe_uniq_array_uninitialized<char>(bit_len);
	bitstring_t output_str(buffer.get(), UnsafeNumericCast<uint32_t>(bit_len));
	Bit::NumericToBit(numeric, output_str);
	return output_str.GetString();
}

template <class T>
T Bit::BitToNumeric(bitstring_t bit) {
	T output;
	Bit::BitToNumeric(bit, output);
	return (output);
}

template <class T>
void Bit::BitToNumeric(bitstring_t bit, T &output_num) {
	D_ASSERT(bit.GetSize() <= sizeof(T) + 1);

	output_num = 0;
	auto data = const_data_ptr_cast(bit.GetData());
	auto output = data_ptr_cast(&output_num);

	idx_t padded_byte_idx = sizeof(T) - bit.GetSize() + 1;
	output[sizeof(T) - 1 - padded_byte_idx] = GetFirstByte(bit);
	for (idx_t idx = padded_byte_idx + 1; idx < sizeof(T); ++idx) {
		output[sizeof(T) - 1 - idx] = data[1 + idx - padded_byte_idx];
	}
	output_num = BSwapIfBE(output_num);
}

} // namespace duckdb
