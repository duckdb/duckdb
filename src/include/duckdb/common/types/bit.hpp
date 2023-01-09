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

	//! Returns the string size of a bit -> string conversion
	DUCKDB_API static idx_t GetStringSize(string_t bits);
	//! Converts bits to a string, writing the output to the designated output string.
	//! The string needs to have space for at least GetStringSize(bits) bytes.
	DUCKDB_API static void ToString(string_t bits, char *output);

	//! Returns the bit size of a string -> bit conversion
	DUCKDB_API static bool TryGetBitSize(string_t str, idx_t &result_size, string *error_message);
//	DUCKDB_API static idx_t GetBlobSize(string_t str);
	//! Convert a string to a bit. This function should ONLY be called after calling GetBitSize, since it does NOT
	//! perform data validation.
	DUCKDB_API static void ToBit(string_t str, data_ptr_t output);
//	//! Convert a string object to a blob
//	DUCKDB_API static string ToBlob(string_t str);
//

};
} // namespace duckdb
