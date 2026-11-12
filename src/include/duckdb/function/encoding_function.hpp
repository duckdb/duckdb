//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/encoding_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//! Decodes a prefix of the encoded input to UTF-8 and returns how many input bytes were consumed.
//! The output has room for input_size * max_output_bytes bytes. Unless is_last is set, an incomplete trailing
//! character is left unconsumed. With is_last set, all input is consumed or an error is thrown.
typedef void (*decode_t)(const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
                         idx_t &written);

//! Describes an encoding scalar function, which is how the CSV reader finds the encodings it can decode.
struct EncodingFunctionInfo : public ScalarFunctionInfo {
	EncodingFunctionInfo(string encoding_name_p, idx_t lookup_bytes_p, idx_t max_output_bytes_p)
	    : encoding_name(std::move(encoding_name_p)), lookup_bytes(lookup_bytes_p),
	      max_output_bytes(max_output_bytes_p) {
	}

	//! The name of the encoding, as passed to e.g. read_csv(encoding := ...)
	string encoding_name;
	//! The maximum number of input bytes that make up a single character
	idx_t lookup_bytes;
	//! The maximum number of UTF-8 bytes produced for a single input byte
	idx_t max_output_bytes;
};

//! An encoding is a set of scalar functions in the system catalog:
//!   decode_<name>(BLOB) -> VARCHAR                     decodes the complete input
//!   decode_<name>(BLOB, BOOLEAN) -> STRUCT(decoded VARCHAR, consumed BIGINT)
//!                                                      decodes a prefix of a stream, see decode_t
struct EncodingFunction {
	static constexpr const char *PREFIX = "decode_";

	//! The name of the scalar function that decodes the encoding
	DUCKDB_API static string FunctionName(const string &encoding_name);
	//! The type returned by the streaming overload
	DUCKDB_API static LogicalType StreamResultType();
	//! Creates the scalar functions for an encoding that is decoded by the given function
	DUCKDB_API static ScalarFunctionSet Create(const string &encoding_name, decode_t decode, idx_t lookup_bytes,
	                                           idx_t max_output_bytes);
};

} // namespace duckdb
