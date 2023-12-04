#include "mbedtls_wrapper.hpp"
// otherwise we have different definitions for mbedtls_pk_context / mbedtls_sha256_context
#define MBEDTLS_ALLOW_PRIVATE_ACCESS

#include "mbedtls/sha256.h"
#include "mbedtls/pk.h"

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


void MbedTlsWrapper::ComputeSha256Hash(const char* in, size_t in_len, char* out) {

	mbedtls_sha256_context sha_context;
	mbedtls_sha256_init(&sha_context);
	if(mbedtls_sha256_starts(&sha_context, false) || mbedtls_sha256_update(&sha_context, (const unsigned char*) in, in_len) || mbedtls_sha256_finish(&sha_context, (unsigned char*)out)) {
		throw runtime_error("SHA256 Error");
	}
	mbedtls_sha256_free(&sha_context);
}

string MbedTlsWrapper::ComputeSha256Hash(const string& file_content) {
	string hash;
	hash.resize(MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
	ComputeSha256Hash(file_content.data(), file_content.size(), (char*)hash.data());
	return hash;
}

bool MbedTlsWrapper::IsValidSha256Signature(const std::string &pubkey, const std::string &signature, const std::string &sha256_hash) {

	if (signature.size() != 256 || sha256_hash.size() != 32) {
		throw std::runtime_error("Invalid input lengths, expected signature length 256, got " + to_string(signature.size()) + ", hash length 32, got " + to_string(sha256_hash.size()));
	}

	mbedtls_pk_context pk_context;
	mbedtls_pk_init(&pk_context);

	if (mbedtls_pk_parse_public_key( &pk_context,
	                                    (const unsigned char*) pubkey.c_str(),pubkey.size() + 1 )) {
		throw runtime_error("RSA public key import error");
	}

	// actually verify
	bool valid = mbedtls_pk_verify(&pk_context, MBEDTLS_MD_SHA256,
	                               (const unsigned char*) sha256_hash.data(), sha256_hash.size(),
	                          (const unsigned char*)signature.data(), signature.length()) == 0;

	mbedtls_pk_free(&pk_context);
	return valid;
}

// used in s3fs
void MbedTlsWrapper::Hmac256(const char* key, size_t key_len, const char* message, size_t message_len, char* out) {
	mbedtls_md_context_t hmac_ctx;
	const mbedtls_md_info_t *md_type = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
	if (!md_type) {
		throw runtime_error("failed to init hmac");
	}

	if (mbedtls_md_setup(&hmac_ctx, md_type, 1) ||
	    mbedtls_md_hmac_starts(&hmac_ctx, (const unsigned char *) key, key_len) ||
	    mbedtls_md_hmac_update(&hmac_ctx, (const unsigned char *)message, message_len) ||
	    mbedtls_md_hmac_finish(&hmac_ctx, (unsigned char *) out)) {
		throw runtime_error("HMAC256 Error");
	}
	mbedtls_md_free(&hmac_ctx);
}

void MbedTlsWrapper::ToBase16(char *in, char *out, size_t len) {
	static char const HEX_CODES[] = "0123456789abcdef";
	size_t i, j;

	for (j = i = 0; i < len; i++) {
		int a = in[i];
		out[j++] = HEX_CODES[(a >> 4) & 0xf];
		out[j++] = HEX_CODES[a & 0xf];
	}
}

MbedTlsWrapper::SHA256State::SHA256State() : sha_context(new mbedtls_sha256_context()) {
	mbedtls_sha256_init((mbedtls_sha256_context*)sha_context);

	if (mbedtls_sha256_starts((mbedtls_sha256_context*)sha_context, false)) {
		throw std::runtime_error("SHA256 Error");
	}
}

MbedTlsWrapper::SHA256State::~SHA256State() {
	mbedtls_sha256_free((mbedtls_sha256_context*)sha_context);
	delete (mbedtls_sha256_context*)sha_context;
}

void MbedTlsWrapper::SHA256State::AddString(const std::string & str) {
	if (mbedtls_sha256_update((mbedtls_sha256_context*)sha_context, (unsigned char*)str.data(), str.size())) {
		throw std::runtime_error("SHA256 Error");
	}
}

std::string MbedTlsWrapper::SHA256State::Finalize() {
	string hash;
	hash.resize(MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);

	if (mbedtls_sha256_finish((mbedtls_sha256_context*)sha_context, (unsigned char*)hash.data())) {
		throw std::runtime_error("SHA256 Error");
	}

	return hash;
}

void MbedTlsWrapper::SHA256State::FinishHex(char *out) {
	string hash;
	hash.resize(MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);

	if (mbedtls_sha256_finish((mbedtls_sha256_context *)sha_context, (unsigned char *)hash.data())) {
		throw std::runtime_error("SHA256 Error");
	}

	MbedTlsWrapper::ToBase16(const_cast<char *>(hash.c_str()), out, MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}
