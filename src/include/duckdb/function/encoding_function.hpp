//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/encoding_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/storage/data_pointer.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

struct DBConfig;

//! Decode function, basically takes information about the decoded and the encoded buffers.
typedef void (*encode_t)(const char *encoded_buffer, idx_t &encoded_buffer_current_position,
                         const idx_t encoded_buffer_size, char *decoded_buffer, idx_t &decoded_buffer_current_position,
                         const idx_t decoded_buffer_size, char *remaining_bytes_buffer, idx_t &remaining_bytes_size);

class EncodingFunction {
public:
	EncodingFunction() : encode_function(nullptr), ratio(0), bytes_per_iteration(0) {
	}

	EncodingFunction(const string &encode_type, encode_t encode_function, const idx_t ratio,
	                 const idx_t bytes_per_iteration)
	    : encoding_type(encode_type), encode_function(encode_function), ratio(ratio),
	      bytes_per_iteration(bytes_per_iteration) {
		D_ASSERT(ratio > 0);
		D_ASSERT(encode_function);
		D_ASSERT(bytes_per_iteration > 0);
	};

	~EncodingFunction() {};

	string GetType() const {
		return encoding_type;
	}
	encode_t GetFunction() const {
		return encode_function;
	}
	idx_t GetRatio() const {
		return ratio;
	}
	idx_t GetBytesPerIteration() const {
		return bytes_per_iteration;
	}

private:
	//! The encoding type of this function (e.g., utf-8)
	string encoding_type;
	//! The actual encoding function
	encode_t encode_function;
	//! Ratio of the max size this encoded buffer could ever reach on a decoded buffer
	idx_t ratio;
	//! How many bytes in the decoded buffer one iteration of the encoded function can cause.
	//! e.g., one iteration of Latin-1 to UTF-8 can generate max 2 bytes.
	//! However, one iteration of UTF-16 to UTF-8, can generate up to 3 UTF-8 bytes.
	idx_t bytes_per_iteration;
};

//! The set of encoding functions
struct EncodingFunctionSet {
	EncodingFunctionSet() {};
	static void Initialize(DBConfig &config);
	mutex lock;
	case_insensitive_map_t<EncodingFunction> functions;
};

} // namespace duckdb
