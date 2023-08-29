//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/crypto/sha256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "mbedtls/sha256.h"

namespace duckdb {

class SHA256Context {
public:
	static constexpr idx_t SHA256_HASH_LENGTH_BINARY = 32;
	static constexpr idx_t SHA256_HASH_LENGTH_TEXT = 64;

public:
	SHA256Context();
	~SHA256Context();

	void Add(string_t str);

	void Finish(char *out);

private:
	mbedtls_sha256_context sha_context;
};

} // namespace duckdb
