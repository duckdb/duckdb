#include "duckdb/common/crypto/sha512.hpp"


namespace duckdb {

SHA512Context::SHA512Context() {
	mbedtls_sha512_init(&sha_context);
	mbedtls_sha512_starts(&sha_context, false);
}

void SHA512Context::Add(string_t str) {
	mbedtls_sha512_update(&sha_context, (const unsigned char *)str.GetData(), str.GetSize());
}

void SHA512Context::Finish(char* out) {
	mbedtls_sha512_finish(&sha_context, (unsigned char *)out);
}

} // namespace duckdb