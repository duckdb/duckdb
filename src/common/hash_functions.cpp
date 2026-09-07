#include "duckdb/common/hash_functions.hpp"

namespace duckdb {

void sha256(EncryptionUtil &encryption_util, const_data_ptr_t in, idx_t in_len, hash_bytes &out) {
	D_ASSERT(CryptoHash::GetDigestSize(CryptoHashFunction::SHA256) == sizeof(hash_bytes));
	encryption_util.Hash(CryptoHashFunction::SHA256, in, in_len, out);
}

void hmac256(EncryptionUtil &encryption_util, const std::string &message, const_data_ptr_t secret, idx_t secret_len,
             hash_bytes &out) {
	D_ASSERT(CryptoHash::GetDigestSize(CryptoHashFunction::SHA256) == sizeof(hash_bytes));
	encryption_util.Hmac(CryptoHashFunction::SHA256, secret, secret_len, const_data_ptr_cast(message.data()),
	                     message.size(), out);
}

void hmac256(EncryptionUtil &encryption_util, const std::string &message, hash_bytes secret, hash_bytes &out) {
	hmac256(encryption_util, message, secret, sizeof(hash_bytes), out);
}

void hex256(hash_bytes &in, hash_str &out) {
	CryptoHash::ToHex(in, sizeof(hash_bytes), char_ptr_cast(out));
}

} // namespace duckdb
