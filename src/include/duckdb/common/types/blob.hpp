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
	// map of integer -> hex value
	static constexpr const char *HEX_TABLE = "0123456789ABCDEF";
	// reverse map of byte -> integer value, or -1 for invalid hex values
	static const int HEX_MAP[256];
public:
	//! Returns the string size of a blob -> string conversion
	static idx_t GetStringSize(string_t blob);
	//! Converts a blob to a string, writing the output to the designated output string.
	//! The string needs to have space for at least GetStringSize(blob) bytes.
	static void ToString(string_t blob, char *output);
	//! Convert a blob object to a string
	static string ToString(string_t blob);

	//! Returns the blob size of a string -> blob conversion
	static idx_t GetBlobSize(string_t str);
	//! Convert a string to a blob. This function should ONLY be called after calling GetBlobSize, since it does NOT perform data validation.
	static void ToBlob(string_t str, data_ptr_t output);
	//! Convert a string object to a blob
	static string ToBlob(string_t str);
};
} // namespace duckdb
