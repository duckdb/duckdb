#include "crypto.hpp"
#include "picohash.hpp"

namespace duckdb {

typedef unsigned char hash_bytes[PICOHASH_SHA256_DIGEST_LENGTH];
typedef unsigned char hash_str[PICOHASH_SHA256_DIGEST_LENGTH * 2];

void sha256(const char *in, size_t in_len, hash_bytes &out) {
	picohash_ctx_t ctx;
	picohash_init_sha256(&ctx);
	picohash_update(&ctx, in, in_len);
	picohash_final(&ctx, out);
}

void hmac256(const std::string &message, const char *secret, size_t secret_len, hash_bytes &out) {
	picohash_ctx_t ctx;
	picohash_init_hmac(&ctx, picohash_init_sha256, secret, secret_len);
	picohash_update(&ctx, message.c_str(), message.length());
	picohash_final(&ctx, out);
}

void hmac256(std::string message, hash_bytes secret, hash_bytes &out) {
	hmac256(message, (char *)secret, sizeof(hash_bytes), out);
}

void hex256(hash_bytes &in, hash_str &out) {
	const char *hex = "0123456789abcdef";
	unsigned char *pin = in;
	unsigned char *pout = out;
	for (; pin < in + sizeof(in); pout += 2, pin++) {
		pout[0] = hex[(*pin >> 4) & 0xF];
		pout[1] = hex[*pin & 0xF];
	}
}
} // namespace duckdb
