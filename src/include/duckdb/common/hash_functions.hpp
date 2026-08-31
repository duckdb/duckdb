#pragma once

#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/helper.hpp"

#include <string>

namespace duckdb {

typedef unsigned char hash_bytes[32];
typedef unsigned char hash_str[64];

void sha256(EncryptionUtil &encryption_util, const_data_ptr_t in, idx_t in_len, hash_bytes &out);

void hmac256(EncryptionUtil &encryption_util, const std::string &message, const_data_ptr_t secret, idx_t secret_len,
             hash_bytes &out);

void hmac256(EncryptionUtil &encryption_util, const std::string &message, hash_bytes secret, hash_bytes &out);

void hex256(hash_bytes &in, hash_str &out);

} // namespace duckdb
