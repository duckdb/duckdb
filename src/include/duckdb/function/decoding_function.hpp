//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/decoding_function.hpp
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
typedef void (*decode_t)(char *encoded_buffer, idx_t &encoded_buffer_current_position, const idx_t encoded_buffer_size,
                         char *decoded_buffer, idx_t &decoded_buffer_current_position, const idx_t decoded_buffer_size,
                         char *remaining_bytes_buffer, idx_t &remaining_bytes_size);

class DecodingFunction {
public:
	DecodingFunction() : decode_function(nullptr), ratio(0), bytes_per_iteration(0) {
	}
	DecodingFunction(const string &decoding_type_p, decode_t decode_function, const idx_t ratio,
	                 const idx_t bytes_per_iteration)
	    : decode_function(decode_function), ratio(ratio), bytes_per_iteration(bytes_per_iteration) {
		decoding_type = StringUtil::Lower(decoding_type_p);
	};
	string GetType() const {
		return decoding_type;
	}
	decode_t GetFunction() const {
		return decode_function;
	}
	idx_t GetRatio() const {
		return ratio;
	}
	idx_t GetBytesPerIteration() const {
		return bytes_per_iteration;
	}

private:
	//! The decoding type of this function (e.g., utf-8)
	string decoding_type;
	//! The actual decoding function
	decode_t decode_function;
	//! Ratio of the max size this encoded buffer could ever reach on a decoded buffer)
	idx_t ratio;
	//! How many bytes one iteration can cause
	idx_t bytes_per_iteration;
};

//! The set of decoding functions
struct DecodingFunctionSet {
	DecodingFunctionSet() {};
	static void Initialize(DBConfig &config);
	mutex lock;
	map<string, DecodingFunction> functions;
};

} // namespace duckdb
