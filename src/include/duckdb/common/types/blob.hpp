//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/blob.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! The Blob class is a static class that holds helper functions for the Blob type.
class Blob {
public:
	// map of integer -> hex value
	static constexpr const char *HEX_TABLE = "0123456789ABCDEF";
	// reverse map of byte -> integer value, or -1 for invalid hex values
	static const int HEX_MAP[256];
	//! map of index -> base64 character
	static constexpr const char *BASE64_MAP = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	//! padding character used in base64 encoding
	static constexpr const char BASE64_PADDING = '=';

public:
	//! Returns the string size of a blob -> string conversion
	DUCKDB_API static idx_t GetStringSize(string_t blob);
	//! Converts a blob to a string, writing the output to the designated output string.
	//! The string needs to have space for at least GetStringSize(blob) bytes.
	DUCKDB_API static void ToString(string_t blob, char *output);
	//! Convert a blob object to a string
	DUCKDB_API static string ToString(string_t blob);

	//! Returns the blob size of a string -> blob conversion
	DUCKDB_API static bool TryGetBlobSize(string_t str, idx_t &result_size, string *error_message);
	DUCKDB_API static idx_t GetBlobSize(string_t str);
	//! Convert a string to a blob. This function should ONLY be called after calling GetBlobSize, since it does NOT
	//! perform data validation.
	DUCKDB_API static void ToBlob(string_t str, data_ptr_t output);
	//! Convert a string object to a blob
	DUCKDB_API static string ToBlob(string_t str);

	// base 64 conversion functions
	//! Returns the string size of a blob -> base64 conversion
	DUCKDB_API static idx_t ToBase64Size(string_t blob);
	//! Converts a blob to a base64 string, output should have space for at least ToBase64Size(blob) bytes
	DUCKDB_API static void ToBase64(string_t blob, char *output);

	//! Returns the string size of a base64 string -> blob conversion
	DUCKDB_API static idx_t FromBase64Size(string_t str);
	//! Converts a base64 string to a blob, output should have space for at least FromBase64Size(blob) bytes
	DUCKDB_API static void FromBase64(string_t str, data_ptr_t output, idx_t output_size);
};
} // namespace duckdb
