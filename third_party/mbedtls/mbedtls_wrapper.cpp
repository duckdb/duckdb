#include "mbedtls_wrapper.hpp"

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


string MbedTlsWrapper::ComputeSha256Hash(const string& file_content) {
	string hash;
	hash.resize(32);

	mbedtls_sha256_context sha_context;
	mbedtls_sha256_init(&sha_context);
	if(mbedtls_sha256_starts(&sha_context, false) || mbedtls_sha256_update(&sha_context, (const unsigned char*) file_content.data(), file_content.size()) || mbedtls_sha256_finish(&sha_context, (unsigned char*)hash.data())) {
		throw runtime_error("SHA256 Error");
	}
	mbedtls_sha256_free(&sha_context);
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
