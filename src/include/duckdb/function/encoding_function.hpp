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

//! Encodes UTF-8 input and returns the number of bytes written. The output has room for
//! input_size * minimum_input_window bytes. Throws if the input has a character the encoding cannot represent.
typedef std::function<void(const char *input, idx_t input_size, char *output, idx_t &written)> encode_t;

class ClientContext;
class ScalarFunctionCatalogEntry;
class Expression;

//! An encoding is a set of scalar functions named after the encoding in the system.encodings schema:
//!   system.encodings."<name>"(BLOB) -> VARCHAR                  decodes the complete input
//!   system.encodings."<name>"(BLOB, BOOLEAN) -> STRUCT(decoded VARCHAR, consumed BIGINT)
//!                                                               decodes a prefix of a stream, see decode_t
//!   system.encodings."<name>"(VARCHAR) -> BLOB                  encodes the input, see encode_t (optional)
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
	DUCKDB_API static ScalarFunctionSet Create(const string &encoding_name, decode_t decode, encode_t encode,
	                                           idx_t minimum_input_window, idx_t max_output_bytes);
	//! Finds the functions of an encoding, autoloading the encodings extension if needed. Returns null if not found.
	DUCKDB_API static optional_ptr<ScalarFunctionCatalogEntry> Find(ClientContext &context,
	                                                                const string &encoding_name);
	//! The sorted names of all encodings, including utf-8
	DUCKDB_API static vector<string> GetEncodings(ClientContext &context);
	//! Binds encode(string, encoding) and decode(blob, encoding) to the function of the encoding. Returns null if the
	//! encoding is not a constant that names an encoding (e.g. an error option of decode).
	DUCKDB_API static unique_ptr<Expression> BindCall(ClientContext &context, vector<unique_ptr<Expression>> &children,
	                                                  bool is_encode);
};

} // namespace duckdb
