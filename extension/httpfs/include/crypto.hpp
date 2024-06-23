#pragma once

#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/helper.hpp"

#include <stddef.h>
#include <string>

typedef struct evp_cipher_ctx_st EVP_CIPHER_CTX;

namespace duckdb {

typedef unsigned char hash_bytes[32];
typedef unsigned char hash_str[64];

void sha256(const char *in, size_t in_len, hash_bytes &out);

void hmac256(const std::string &message, const char *secret, size_t secret_len, hash_bytes &out);

void hmac256(std::string message, hash_bytes secret, hash_bytes &out);

void hex256(hash_bytes &in, hash_str &out);

class AESGCMStateSSL : public EncryptionState {

public:
	DUCKDB_API explicit AESGCMStateSSL();
	DUCKDB_API ~AESGCMStateSSL() override;

public:
	DUCKDB_API bool IsOpenSSL() override;
	DUCKDB_API void InitializeEncryption(const_data_ptr_t iv, idx_t iv_len, const std::string *key) override;
	DUCKDB_API void InitializeDecryption(const_data_ptr_t iv, idx_t iv_len, const std::string *key) override;
	DUCKDB_API size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len) override;
	DUCKDB_API size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len) override;
	DUCKDB_API void GenerateRandomData(data_ptr_t data, idx_t len) override;

private:
	bool ssl = true;
	EVP_CIPHER_CTX *gcm_context;
	Mode mode;
};

class AESGCMStateSSLFactory : public EncryptionUtil {
public:
	DUCKDB_API explicit AESGCMStateSSLFactory() {
	}

	DUCKDB_API shared_ptr<EncryptionState> CreateEncryptionState() const override {
		return make_shared_ptr<AESGCMStateSSL>();
	}

	DUCKDB_API ~AESGCMStateSSLFactory() override {
	} //
};

} // namespace duckdb
