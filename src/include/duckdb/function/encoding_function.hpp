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

class EncodingFunction;

struct CSVEncoderBuffer;
//! Decode function, basically takes information about the decoded and the encoded buffers.
typedef void (*encode_t)(CSVEncoderBuffer &encoded_buffer, char *decoded_buffer, idx_t &decoded_buffer_current_position,
                         const idx_t decoded_buffer_size, char *remaining_bytes_buffer, idx_t &remaining_bytes_size,
                         EncodingFunction *encoding_function);

class EncodingFunction {
public:
	EncodingFunction() : encode_function(nullptr), max_bytes_per_iteration(0) {
	}

	EncodingFunction(const string &encode_name, encode_t encode_function, const idx_t bytes_per_iteration,
	                 const idx_t lookup_bytes)
	    : name(encode_name), encode_function(encode_function), max_bytes_per_iteration(bytes_per_iteration),
	      lookup_bytes(lookup_bytes) {
		D_ASSERT(encode_function);
		D_ASSERT(bytes_per_iteration > 0);
		D_ASSERT(lookup_bytes > 0);
	};
	EncodingFunction(const string &encode_name, encode_t encode_function, const idx_t bytes_per_iteration,
	                 const idx_t lookup_bytes, const uintptr_t map, const size_t map_size)
	    : conversion_map(map), map_size(map_size), name(encode_name), encode_function(encode_function),
	      max_bytes_per_iteration(bytes_per_iteration), lookup_bytes(lookup_bytes) {
		D_ASSERT(encode_function);
		D_ASSERT(bytes_per_iteration > 0);
		D_ASSERT(lookup_bytes > 0);
	};

	~EncodingFunction() {};

	string GetName() const {
		return name;
	}
	encode_t GetFunction() const {
		return encode_function;
	}
	idx_t GetBytesPerIteration() const {
		return max_bytes_per_iteration;
	}
	idx_t GetLookupBytes() const {
		return lookup_bytes;
	}

	//! Optional convertion map, that indicates byte replacements.
	uintptr_t conversion_map {};
	size_t map_size {};

protected:
	//! The encoding type of this function (e.g., utf-8)
	string name;
	//! The actual encoding function
	encode_t encode_function;
	//! How many bytes in the decoded buffer one iteration of the encoded function can cause.
	//! e.g., one iteration of Latin-1 to UTF-8 can generate max 2 bytes.
	//! However, one iteration of UTF-16 to UTF-8, can generate up to 3 UTF-8 bytes.
	idx_t max_bytes_per_iteration;
	//! How many bytes we have to lookup before knowing the bytes we have to output
	idx_t lookup_bytes = 1;
};

//! The set of encoding functions
struct EncodingFunctionSet {
	EncodingFunctionSet() {};
	static void Initialize(DBConfig &config);
	mutex lock;
	case_insensitive_map_t<EncodingFunction> functions;
};

} // namespace duckdb
