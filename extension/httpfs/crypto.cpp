#include "crypto.hpp"
#include "mbedtls_wrapper.hpp"
#include <iostream>
#include "duckdb/common/common.hpp"
#include <stdio.h>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

namespace duckdb {

void sha256(const char *in, size_t in_len, hash_bytes &out) {
	duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(in, in_len, (char *)out);
}

void hmac256(const std::string &message, const char *secret, size_t secret_len, hash_bytes &out) {
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(secret, secret_len, message.data(), message.size(), (char *)out);
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

const EVP_CIPHER *GetCipher(const string &key) {
	// For now, we only support GCM ciphers
	switch (key.size()) {
	case 16:
		return EVP_aes_128_gcm();
	case 24:
		return EVP_aes_192_gcm();
	case 32:
		return EVP_aes_256_gcm();
	default:
		throw InternalException("Invalid AES key length");
	}
}

AESGCMStateSSL::AESGCMStateSSL() : gcm_context(EVP_CIPHER_CTX_new()) {
	if (!(gcm_context)) {
		throw InternalException("AES GCM failed with initializing context");
	}
}

AESGCMStateSSL::~AESGCMStateSSL() {
	// Clean up
	EVP_CIPHER_CTX_free(gcm_context);
}

bool AESGCMStateSSL::IsOpenSSL() {
	return ssl;
}

void AESGCMStateSSL::GenerateRandomData(data_ptr_t data, idx_t len) {
	// generate random bytes for nonce
	RAND_bytes(data, len);
}

void AESGCMStateSSL::InitializeEncryption(const_data_ptr_t iv, idx_t iv_len, const string *key) {
	mode = ENCRYPT;

	if (1 != EVP_EncryptInit_ex(gcm_context, GetCipher(*key), NULL, const_data_ptr_cast(key->data()), iv)) {
		throw InternalException("EncryptInit failed");
	}
}

void AESGCMStateSSL::InitializeDecryption(const_data_ptr_t iv, idx_t iv_len, const string *key) {
	mode = DECRYPT;

	if (1 != EVP_DecryptInit_ex(gcm_context, GetCipher(*key), NULL, const_data_ptr_cast(key->data()), iv)) {
		throw InternalException("DecryptInit failed");
	}
}

size_t AESGCMStateSSL::Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len) {

	switch (mode) {
	case ENCRYPT:
		if (1 != EVP_EncryptUpdate(gcm_context, data_ptr_cast(out), reinterpret_cast<int *>(&out_len),
		                           const_data_ptr_cast(in), (int)in_len)) {
			throw InternalException("EncryptUpdate failed");
		}
		break;

	case DECRYPT:
		if (1 != EVP_DecryptUpdate(gcm_context, data_ptr_cast(out), reinterpret_cast<int *>(&out_len),
		                           const_data_ptr_cast(in), (int)in_len)) {

			throw InternalException("DecryptUpdate failed");
		}
		break;
	}

	if (out_len != in_len) {
		throw InternalException("AES GCM failed, in- and output lengths differ");
	}

	return out_len;
}

size_t AESGCMStateSSL::Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len) {
	auto text_len = out_len;

	switch (mode) {
	case ENCRYPT:
		if (1 != EVP_EncryptFinal_ex(gcm_context, data_ptr_cast(out) + out_len, reinterpret_cast<int *>(&out_len))) {
			throw InternalException("EncryptFinal failed");
		}
		text_len += out_len;
		// The computed tag is written at the end of a chunk
		if (1 != EVP_CIPHER_CTX_ctrl(gcm_context, EVP_CTRL_GCM_GET_TAG, tag_len, tag)) {
			throw InternalException("Calculating the tag failed");
		}
		return text_len;
	case DECRYPT:
		// Set expected tag value
		if (!EVP_CIPHER_CTX_ctrl(gcm_context, EVP_CTRL_GCM_SET_TAG, tag_len, tag)) {
			throw InternalException("Finalizing tag failed");
		}
		// EVP_DecryptFinal() will return an error code if final block is not correctly formatted.
		int ret = EVP_DecryptFinal_ex(gcm_context, data_ptr_cast(out) + out_len, reinterpret_cast<int *>(&out_len));
		text_len += out_len;

		if (ret > 0) {
			// success
			return text_len;
		}
		throw InvalidInputException("Computed AES tag differs from read AES tag, are you using the right key?");
	}
}

} // namespace duckdb

extern "C" {

// Call the member function through the factory object
DUCKDB_EXTENSION_API AESGCMStateSSLFactory *CreateSSLFactory() {
	return new AESGCMStateSSLFactory();
};
}
