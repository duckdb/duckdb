#include "mbedtls_wrapper.hpp"
// otherwise we have different definitions for mbedtls_pk_context / mbedtls_sha256_context
#define MBEDTLS_ALLOW_PRIVATE_ACCESS

#include "duckdb/common/helper.hpp"
#include "mbedtls/aes.h"
#include "mbedtls/gcm.h"
#include "mbedtls/pk.h"
#include "mbedtls/sha256.h"

#include <stdexcept>

using namespace std;
using namespace duckdb_mbedtls;

/*
# Command line tricks to help here
# Create a new key
openssl genrsa -out private.pem 2048

# Export public key
openssl rsa -in private.pem -outform PEM -pubout -out public.pem

# Calculate digest and write to 'hash' file on command line
openssl dgst -binary -sha256 dummy > hash

# Calculate signature from hash
openssl pkeyutl -sign -in hash -inkey private.pem -pkeyopt digest:sha256 -out dummy.sign
*/

void MbedTlsWrapper::ComputeSha256Hash(const char *in, size_t in_len, char *out) {

	mbedtls_sha256_context sha_context;
	mbedtls_sha256_init(&sha_context);
	if (mbedtls_sha256_starts(&sha_context, false) ||
	    mbedtls_sha256_update(&sha_context, reinterpret_cast<const unsigned char *>(in), in_len) ||
	    mbedtls_sha256_finish(&sha_context, reinterpret_cast<unsigned char *>(out))) {
		throw runtime_error("SHA256 Error");
	}
	mbedtls_sha256_free(&sha_context);
}

string MbedTlsWrapper::ComputeSha256Hash(const string &file_content) {
	string hash;
	hash.resize(MbedTlsWrapper::SHA256_HASH_BYTES);
	ComputeSha256Hash(file_content.data(), file_content.size(), (char *)hash.data());
	return hash;
}

bool MbedTlsWrapper::IsValidSha256Signature(const std::string &pubkey, const std::string &signature,
                                            const std::string &sha256_hash) {

	if (signature.size() != 256 || sha256_hash.size() != 32) {
		throw std::runtime_error("Invalid input lengths, expected signature length 256, got " +
		                         to_string(signature.size()) + ", hash length 32, got " +
		                         to_string(sha256_hash.size()));
	}

	mbedtls_pk_context pk_context;
	mbedtls_pk_init(&pk_context);

	if (mbedtls_pk_parse_public_key(&pk_context, reinterpret_cast<const unsigned char *>(pubkey.c_str()),
	                                pubkey.size() + 1)) {
		throw runtime_error("RSA public key import error");
	}

	// actually verify
	bool valid = mbedtls_pk_verify(&pk_context, MBEDTLS_MD_SHA256,
	                               reinterpret_cast<const unsigned char *>(sha256_hash.data()), sha256_hash.size(),
	                               reinterpret_cast<const unsigned char *>(signature.data()), signature.length()) == 0;

	mbedtls_pk_free(&pk_context);
	return valid;
}

// used in s3fs
void MbedTlsWrapper::Hmac256(const char *key, size_t key_len, const char *message, size_t message_len, char *out) {
	mbedtls_md_context_t hmac_ctx;
	const mbedtls_md_info_t *md_type = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
	if (!md_type) {
		throw runtime_error("failed to init hmac");
	}

	if (mbedtls_md_setup(&hmac_ctx, md_type, 1) ||
	    mbedtls_md_hmac_starts(&hmac_ctx, reinterpret_cast<const unsigned char *>(key), key_len) ||
	    mbedtls_md_hmac_update(&hmac_ctx, reinterpret_cast<const unsigned char *>(message), message_len) ||
	    mbedtls_md_hmac_finish(&hmac_ctx, reinterpret_cast<unsigned char *>(out))) {
		throw runtime_error("HMAC256 Error");
	}
	mbedtls_md_free(&hmac_ctx);
}

MbedTlsGcmContext::MbedTlsGcmContext() {
}

MbedTlsGcmContext::MbedTlsGcmContext(const std::string &key) {
	Initialize(key);
}

void MbedTlsGcmContext::Initialize(const std::string &key) {
	context_ptr = reinterpret_cast<char *>(malloc(sizeof(mbedtls_gcm_context)));
	auto context = reinterpret_cast<mbedtls_gcm_context *>(context_ptr.get());
	mbedtls_gcm_init(context);
	if (mbedtls_gcm_setkey(context, MBEDTLS_CIPHER_ID_AES, reinterpret_cast<const unsigned char *>(key.c_str()),
	                       key.length() * 8) != 0) {
		throw runtime_error("Invalid AES key length");
	}
}

MbedTlsGcmContext::~MbedTlsGcmContext() {
	if (!context_ptr) {
		return;
	}
	auto context = reinterpret_cast<mbedtls_gcm_context *>(context_ptr.get());
	mbedtls_gcm_free(context);
	free(context_ptr.get());
}

void MbedTlsGcmContext::InitializeDecryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len) {
	auto context = reinterpret_cast<mbedtls_gcm_context *>(context_ptr.get());
	if (mbedtls_gcm_starts(context, MBEDTLS_GCM_DECRYPT, iv, iv_len) != 0) {
		throw runtime_error("Unable to initialize AES decryption");
	}
}

size_t MbedTlsGcmContext::Process(duckdb::const_data_ptr_t in, duckdb::idx_t in_len, duckdb::data_ptr_t out,
                                  duckdb::idx_t out_len) {
	auto context = reinterpret_cast<mbedtls_gcm_context *>(context_ptr.get());
	size_t result;
	if (mbedtls_gcm_update(context, in, in_len, out, out_len, &result) != 0) {
		throw runtime_error("Unable to process using AES");
	}
	return result;
}

// MbedTlsAesContext MbedTlsAesContext::CreateEncryptionContext(const std::string &key) {
//	MbedTlsAesContext result;
//	result.context_ptr = malloc(sizeof(mbedtls_aes_context));
//	auto context = reinterpret_cast<mbedtls_aes_context *>(result.context_ptr);
//	mbedtls_aes_init(context);
//	if (mbedtls_aes_setkey_enc(context, reinterpret_cast<const unsigned char *>(key.c_str()), key.length() * 8) != 0) {
//		throw runtime_error("Invalid AES key length");
//	}
//	return result;
// }
//
// MbedTlsAesContext MbedTlsAesContext::CreateDecryptionContext(const std::string &key) {
//	MbedTlsAesContext result;
//	result.context_ptr = malloc(sizeof(mbedtls_aes_context));
//	auto context = reinterpret_cast<mbedtls_aes_context *>(result.context_ptr);
//	mbedtls_aes_init(context);
//	if (mbedtls_aes_setkey_dec(context, reinterpret_cast<const unsigned char *>(key.c_str()), key.length() * 8) != 0) {
//		throw runtime_error("Invalid AES key length");
//	}
//	return result;
// }
//
// MbedTlsAesContext::~MbedTlsAesContext() {
//	mbedtls_aes_free(reinterpret_cast<mbedtls_aes_context *>(context_ptr));
//	free(context_ptr);
// }
//
// void MbedTlsWrapper::Encrypt(MbedTlsAesContext &context, unsigned char iv[16], unsigned char *in, size_t in_len) {
//	if (mbedtls_aes_crypt_cbc(reinterpret_cast<mbedtls_aes_context *>(context.context_ptr), MBEDTLS_AES_ENCRYPT, in_len,
//	                          iv, in, in) != 0) {
//		throw runtime_error("Invalid AES input length");
//	}
// }
//
// void MbedTlsWrapper::Decrypt(MbedTlsAesContext &context, unsigned char iv[16], unsigned char *in, size_t in_len) {
//	if (mbedtls_aes_crypt_cbc(reinterpret_cast<mbedtls_aes_context *>(context.context_ptr), MBEDTLS_AES_DECRYPT, in_len,
//	                          iv, in, in) != 0) {
//		throw runtime_error("Invalid AES input length");
//	}
// }
