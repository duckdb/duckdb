#include "duckdb/common/binary_util.hpp"
#include "duckdb/common/crypto/sha256.hpp"

#define MBEDTLS_ALLOW_PRIVATE_ACCESS

namespace duckdb {

SHA256Context::SHA256Context() {
	mbedtls_sha256_init(&sha_context);
	mbedtls_sha256_starts(&sha_context, false);
}

SHA256Context::~SHA256Context() {
	mbedtls_sha256_free(&sha_context);
}

void SHA256Context::Add(string_t str) {
	mbedtls_sha256_update(&sha_context, reinterpret_cast<const unsigned char *>(str.GetData()), str.GetSize());
}

void SHA256Context::Finish(char *out) {
	char digest[SHA256_HASH_LENGTH_BINARY];
	mbedtls_sha256_finish(&sha_context, reinterpret_cast<unsigned char *>(digest));
	BinaryUtil::ToBase16(digest, out, SHA256_HASH_LENGTH_BINARY);
}

} // namespace duckdb
