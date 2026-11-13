//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/encoding_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include <functional>
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

//! Decodes a prefix of the encoded input to UTF-8 and returns how many input bytes were consumed.
//! The output has room for input_size * max_output_bytes bytes. Unless is_last is set, an incomplete trailing
//! character is left unconsumed. With is_last set, all input is consumed or an error is thrown.
typedef std::function<void(const char *input, idx_t input_size, bool is_last, char *output, idx_t &consumed,
                           idx_t &written)>
    decode_t;

//! An encoding is a set of scalar functions named after the encoding in the system.encodings schema:
//!   system.encodings."<name>"(BLOB) -> VARCHAR                  decodes the complete input
//!   system.encodings."<name>"(BLOB, BOOLEAN) -> STRUCT(decoded VARCHAR, consumed BIGINT)
//!                                                               decodes a prefix of a stream, see decode_t
struct EncodingFunction {
	static constexpr const char *SCHEMA = "encodings";
	//! Integer function property: the maximum number of input bytes that make up one character
	static constexpr const char *MINIMUM_INPUT_WINDOW_PROPERTY = "duckdb.encodings.minimum_input_window";

	//! The name of the scalar function that decodes the encoding
	DUCKDB_API static string FunctionName(const string &encoding_name);
	//! Registers the functions of an encoding in the system.encodings schema
	DUCKDB_API static void SetSchema(CreateFunctionInfo &info);
	//! The type returned by the streaming overload
	DUCKDB_API static LogicalType StreamResultType();
	//! Creates the scalar functions for an encoding that is decoded by the given function
	DUCKDB_API static ScalarFunctionSet Create(const string &encoding_name, decode_t decode, idx_t minimum_input_window,
	                                           idx_t max_output_bytes);
};

} // namespace duckdb
