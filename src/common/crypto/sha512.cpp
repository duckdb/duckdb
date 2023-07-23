
#include "duckdb/common/crypto/sha512.hpp"

#include "duckdb/common/binary_util.hpp"

#define MBEDTLS_ALLOW_PRIVATE_ACCESS

namespace duckdb {

SHA512Context::SHA512Context() {
	mbedtls_sha512_init(&sha_context);
	mbedtls_sha512_starts(&sha_context, false);
}

SHA512Context::~SHA512Context() {
	mbedtls_sha512_free(&sha_context);
}

void SHA512Context::Add(string_t str) {
	mbedtls_sha512_update(&sha_context, reinterpret_cast<const unsigned char *>(str.GetData()), str.GetSize());
}

void SHA512Context::Finish(char *out) {
	char digest[SHA512_HASH_LENGTH_BINARY];
	mbedtls_sha512_finish(&sha_context, reinterpret_cast<unsigned char *>(digest));
	BinaryUtil::ToBase16(digest, out, SHA512_HASH_LENGTH_BINARY);
}

} // namespace duckdb
