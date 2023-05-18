//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/crypto/sha512.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "mbedtls/sha512.h"

namespace duckdb {

class SHA512Context {
public:
	static constexpr idx_t SHA512_HASH_LENGTH_BINARY = 512;

public:
	SHA512Context();

	void Add(string_t str);

	//! Write the 512-byte (binary) digest to the specified location
	void Finish(char* out);

private:
	mbedtls_sha512_context sha_context;
};

} // namespace duckdb
