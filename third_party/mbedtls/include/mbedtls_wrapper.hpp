//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mbedtls_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

namespace duckdb_mbedtls {
class MbedTlsWrapper {
public:
	static std::string ComputeSha256Hash(const std::string& file_content);
	static bool IsValidSha256Signature(const std::string& pubkey, const std::string& signature, const std::string& sha256_hash);
};
}
