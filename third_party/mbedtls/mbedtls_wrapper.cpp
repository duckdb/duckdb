#include "mbedtls_wrapper.hpp"

// otherwise we have different definitions for mbedtls_pk_context / mbedtls_sha256_context
#define MBEDTLS_ALLOW_PRIVATE_ACCESS

#include "duckdb/common/helper.hpp"
#include "mbedtls/md.h"
#include "mbedtls/pk.h"
#include "mbedtls/sha1.h"
#include "mbedtls/sha256.h"
#include "mbedtls/cipher.h"

#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/timestamp.hpp"

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
	hash.resize(MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
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
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);

	mbedtls_sha256_init(context);

	if (mbedtls_sha256_starts(context, false)) {
		throw std::runtime_error("SHA256 Error");
	}
}

MbedTlsWrapper::SHA256State::~SHA256State() {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);
	mbedtls_sha256_free(context);
	delete context;
}

void MbedTlsWrapper::SHA256State::AddString(const std::string &str) {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);
	if (mbedtls_sha256_update(context, (unsigned char *)str.data(), str.size())) {
		throw std::runtime_error("SHA256 Error");
	}
}

void MbedTlsWrapper::SHA256State::AddBytes(duckdb::const_data_ptr_t input_bytes, duckdb::idx_t len) {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);
	if (mbedtls_sha256_update(context, input_bytes, len)) {
		throw std::runtime_error("SHA256 Error");
	}
}

void MbedTlsWrapper::SHA256State::AddBytes(duckdb::data_ptr_t input_bytes, duckdb::idx_t len) {
	AddBytes(duckdb::const_data_ptr_t(input_bytes), len);
}

void MbedTlsWrapper::SHA256State::AddSalt(unsigned char *salt, size_t salt_len) {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);
	if (mbedtls_sha256_update(context, salt, salt_len)) {
		throw std::runtime_error("SHA256 Error");
	}
}

void MbedTlsWrapper::SHA256State::FinalizeDerivedKey(duckdb::data_ptr_t hash) {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);

	if (mbedtls_sha256_finish(context, (duckdb::data_ptr_t)hash)) {
		throw std::runtime_error("SHA256 Error");
	}
}

std::string MbedTlsWrapper::SHA256State::Finalize() {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);

	string hash;
	hash.resize(MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);

	if (mbedtls_sha256_finish(context, (unsigned char *)hash.data())) {
		throw std::runtime_error("SHA256 Error");
	}

	return hash;
}

void MbedTlsWrapper::SHA256State::FinishHex(char *out) {
	auto context = reinterpret_cast<mbedtls_sha256_context *>(sha_context);

	string hash;
	hash.resize(MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);

	if (mbedtls_sha256_finish(context, (unsigned char *)hash.data())) {
		throw std::runtime_error("SHA256 Error");
	}

	MbedTlsWrapper::ToBase16(const_cast<char *>(hash.c_str()), out, MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}

MbedTlsWrapper::SHA1State::SHA1State() : sha_context(new mbedtls_sha1_context()) {
	auto context = reinterpret_cast<mbedtls_sha1_context *>(sha_context);

	mbedtls_sha1_init(context);

	if (mbedtls_sha1_starts(context)) {
		throw std::runtime_error("SHA1 Error");
	}
}

MbedTlsWrapper::SHA1State::~SHA1State() {
	auto context = reinterpret_cast<mbedtls_sha1_context *>(sha_context);
	mbedtls_sha1_free(context);
	delete context;
}

void MbedTlsWrapper::SHA1State::AddString(const std::string &str) {
	auto context = reinterpret_cast<mbedtls_sha1_context *>(sha_context);
	if (mbedtls_sha1_update(context, (unsigned char *)str.data(), str.size())) {
		throw std::runtime_error("SHA1 Error");
	}
}

std::string MbedTlsWrapper::SHA1State::Finalize() {
	auto context = reinterpret_cast<mbedtls_sha1_context *>(sha_context);

	string hash;
	hash.resize(MbedTlsWrapper::SHA1_HASH_LENGTH_BYTES);

	if (mbedtls_sha1_finish(context, (unsigned char *)hash.data())) {
		throw std::runtime_error("SHA1 Error");
	}

	return hash;
}

void MbedTlsWrapper::SHA1State::FinishHex(char *out) {
	auto context = reinterpret_cast<mbedtls_sha1_context *>(sha_context);

	string hash;
	hash.resize(MbedTlsWrapper::SHA1_HASH_LENGTH_BYTES);

	if (mbedtls_sha1_finish(context, (unsigned char *)hash.data())) {
		throw std::runtime_error("SHA1 Error");
	}

	MbedTlsWrapper::ToBase16(const_cast<char *>(hash.c_str()), out, MbedTlsWrapper::SHA1_HASH_LENGTH_BYTES);
}

const mbedtls_cipher_info_t *MbedTlsWrapper::AESStateMBEDTLS::GetCipher(size_t key_len){

	switch(cipher){
		case duckdb::EncryptionTypes::CipherType::GCM:
		    switch (key_len) {
		    case 16:
			    return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_128_GCM);
		    case 24:
			    return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_192_GCM);
		    case 32:
			    return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_256_GCM);
		    default:
			    throw runtime_error("Invalid AES key length for GCM");
		    }
		case duckdb::EncryptionTypes::CipherType::CTR:
		    switch (key_len) {
		    case 16:
			    return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_128_CTR);
		    case 24:
			    return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_192_CTR);
		    case 32:
			    return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_256_CTR);
		    default:
			    throw runtime_error("Invalid AES key length for CTR");
		    }
		case duckdb::EncryptionTypes::CipherType::CBC:
			switch (key_len) {
			case 16:
				return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_128_CBC);
			case 24:
				return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_192_CBC);
			case 32:
				return mbedtls_cipher_info_from_type(MBEDTLS_CIPHER_AES_256_CBC);
			default:
				throw runtime_error("Invalid AES key length for CBC");
			}
		default:
				throw duckdb::InternalException("Invalid Encryption/Decryption Cipher: %s", duckdb::EncryptionTypes::CipherToString(cipher));
	}
}

void MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(duckdb::data_ptr_t data, duckdb::idx_t len) {
	mbedtls_platform_zeroize(data, len);
}

MbedTlsWrapper::AESStateMBEDTLS::AESStateMBEDTLS(duckdb::EncryptionTypes::CipherType cipher_p, duckdb::idx_t key_len) : EncryptionState(cipher_p, key_len), context(duckdb::make_uniq<mbedtls_cipher_context_t>()) {
	mbedtls_cipher_init(context.get());

	auto cipher_info = GetCipher(key_len);

	if (!cipher_info) {
		throw runtime_error("Failed to get Cipher");
	}

	if (mbedtls_cipher_setup(context.get(), cipher_info)) {
		throw runtime_error("Failed to initialize cipher context");
	}

	if (cipher == duckdb::EncryptionTypes::CBC && mbedtls_cipher_set_padding_mode(context.get(), MBEDTLS_PADDING_PKCS7)) {
		throw runtime_error("Failed to set CBC padding");

	}
}

MbedTlsWrapper::AESStateMBEDTLS::~AESStateMBEDTLS() {
	if (context) {
		mbedtls_cipher_free(context.get());
	}
}

static void ThrowInsecureRNG() {
	throw duckdb::InvalidConfigurationException("DuckDB requires a secure random engine to be loaded to enable secure crypto. Normally, this will be handled automatically by DuckDB by autoloading the `httpfs` Extension, but that seems to have failed. Please ensure the httpfs extension is loaded manually using `LOAD httpfs`.");
}

void MbedTlsWrapper::AESStateMBEDTLS::GenerateRandomData(duckdb::data_ptr_t data, duckdb::idx_t len) {
	ThrowInsecureRNG();
}

void MbedTlsWrapper::AESStateMBEDTLS::InitializeInternal(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, duckdb::const_data_ptr_t aad, duckdb::idx_t aad_len){
	if (mbedtls_cipher_set_iv(context.get(), iv, iv_len)) {
		throw runtime_error("Failed to set IV for encryption");
	}

	if (aad_len > 0) {
		if (mbedtls_cipher_update_ad(context.get(), aad, aad_len)) {
			throw std::runtime_error("Failed to set AAD");
		}
	}
}

void MbedTlsWrapper::AESStateMBEDTLS::InitializeEncryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, duckdb::const_data_ptr_t key, duckdb::idx_t key_len_p, duckdb::const_data_ptr_t aad, duckdb::idx_t aad_len) {
	ThrowInsecureRNG();
}

void MbedTlsWrapper::AESStateMBEDTLS::InitializeDecryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, duckdb::const_data_ptr_t key, duckdb::idx_t key_len_p, duckdb::const_data_ptr_t aad, duckdb::idx_t aad_len) {
	mode = duckdb::EncryptionTypes::DECRYPT;

	if (key_len_p != key_len) {
		throw duckdb::InternalException("Invalid encryption key length, expected %llu, got %llu", key_len, key_len_p);
	}
	if (mbedtls_cipher_setkey(context.get(), key, key_len * 8, MBEDTLS_DECRYPT)) {
		throw runtime_error("Failed to set AES key for encryption");
	}

	InitializeInternal(iv, iv_len, aad, aad_len);
}

size_t MbedTlsWrapper::AESStateMBEDTLS::Process(duckdb::const_data_ptr_t in, duckdb::idx_t in_len, duckdb::data_ptr_t out,
                                                   duckdb::idx_t out_len) {

	// GCM works in-place, CTR and CBC don't
	auto use_out_copy = in == out && cipher != duckdb::EncryptionTypes::CipherType::GCM;

	auto out_ptr = out;
	std::unique_ptr<duckdb::data_t[]> out_copy;
	if (use_out_copy) {
		out_copy.reset(new duckdb::data_t[out_len]);
		out_ptr = out_copy.get();
	}

	size_t out_len_res = duckdb::NumericCast<size_t>(out_len);
	if (mbedtls_cipher_update(context.get(), reinterpret_cast<const unsigned char *>(in), in_len, out_ptr,
	                      &out_len_res)) {
			throw runtime_error("Encryption or Decryption failed at Process");
		};

	if (use_out_copy) {
		memcpy(out, out_ptr, out_len_res);
	}
	return out_len_res;
}

void MbedTlsWrapper::AESStateMBEDTLS::FinalizeGCM(duckdb::data_ptr_t tag, duckdb::idx_t tag_len){

	switch (mode) {

	case duckdb::EncryptionTypes::ENCRYPT: {
		if (mbedtls_cipher_write_tag(context.get(), tag, tag_len)) {
			throw runtime_error("Writing tag failed");
		}
		break;
	}

	case duckdb::EncryptionTypes::DECRYPT: {
		if (mbedtls_cipher_check_tag(context.get(), tag, tag_len)) {
			throw duckdb::InvalidInputException(
			    "Computed AES tag differs from read AES tag, are you using the right key?");
		}
		break;
	}

	default:
		throw duckdb::InternalException("Unhandled encryption mode %d", static_cast<int>(mode));
	}
}

size_t MbedTlsWrapper::AESStateMBEDTLS::Finalize(duckdb::data_ptr_t out, duckdb::idx_t out_len, duckdb::data_ptr_t tag,
													duckdb::idx_t tag_len) {
	size_t result = out_len;
	if (mbedtls_cipher_finish(context.get(), out, &result)) {
		throw runtime_error("Encryption or Decryption failed at Finalize");
	}
	if (cipher == duckdb::EncryptionTypes::GCM) {
		FinalizeGCM(tag, tag_len);
	}
	return result;
}
