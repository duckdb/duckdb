#include "duckdb/common/crypto/sha512.hpp"


namespace duckdb {

SHA512Context::SHA512Context() {
	mbedtls_sha512_init(&sha_context);
	mbedtls_sha512_starts(&sha_context, false);
}

SHA512Context::~SHA512Context() {
	mbedtls_sha512_free(&sha_context);
}

void SHA512Context::Add(string_t str) {
	mbedtls_sha512_update(&sha_context, (const unsigned char *)str.GetData(), str.GetSize());
}

void SHA512Context::Finish(char* out) {
	unsigned char digest[SHA512_HASH_LENGTH_BINARY];
	mbedtls_sha512_finish(&sha_context, digest);
	DigestToBase16(digest, out);
}

void SHA512Context::DigestToBase16(const_data_ptr_t digest, char *zbuf) {
	static char const HEX_CODES[] = "0123456789abcdef";
	int i, j;

	for (j = i = 0; i < 64; i++) {
		int a = digest[i];
		zbuf[j++] = HEX_CODES[(a >> 4) & 0xf];
		zbuf[j++] = HEX_CODES[a & 0xf];
	}
}

} // namespace duckdb