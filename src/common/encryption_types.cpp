#include "duckdb/common/encryption_types.hpp"

namespace duckdb {

static constexpr EncryptionTypes::EncryptionVersion MAX_VERSION = EncryptionTypes::V0_1;

string EncryptionTypes::CipherToString(CipherType cipher_p) {
	switch (cipher_p) {
	case GCM:
		return "GCM";
	case CTR:
		return "CTR";
	case CBC:
		return "CBC";
	default:
		return "INVALID";
	}
}

EncryptionTypes::CipherType EncryptionTypes::StringToCipher(const string &encryption_cipher_p) {
	auto encryption_cipher = StringUtil::Upper(encryption_cipher_p);
	if (encryption_cipher == "GCM") {
		return GCM;
	}
	if (encryption_cipher == "CTR") {
		return CTR;
	}
	if (encryption_cipher == "CBC") {
		throw NotImplementedException("CBC encryption is disabled");
	}
	return INVALID;
}

EncryptionTypes::EncryptionVersion EncryptionTypes::StringToVersion(const string &encryption_version_p) {
	if (encryption_version_p == "v0") {
		return V0_0;
	} else if (encryption_version_p == "v1") {
		return V0_1;
	} else {
		throw NotImplementedException("No encryption version higher then v%d is supported yet in this DuckDB version",
		                              MAX_VERSION);
	}
}

string EncryptionTypes::KDFToString(KeyDerivationFunction kdf_p) {
	switch (kdf_p) {
	case SHA256:
		return "SHA256";
	case PBKDF2:
		return "PBKDF2";
	default:
		return "DEFAULT";
	}
}

EncryptionTypes::KeyDerivationFunction EncryptionTypes::StringToKDF(const string &key_derivation_function_p) {
	auto key_derivation_function = StringUtil::Upper(key_derivation_function_p);
	if (key_derivation_function == "SHA256") {
		return SHA256;
	}
	if (key_derivation_function == "PBKDF2") {
		return PBKDF2;
	}
	return DEFAULT;
}

using CipherType = EncryptionTypes::CipherType;
using Version = EncryptionTypes::EncryptionVersion;

EncryptionTag::EncryptionTag() : tag(new data_t[MainHeader::AES_TAG_LEN]()) {
	tag_len = MainHeader::AES_TAG_LEN;
}

data_ptr_t EncryptionTag::data() {
	return tag.get();
}

idx_t EncryptionTag::size() const {
	return tag_len;
}

bool EncryptionTag::IsAllZeros() const {
	auto tag_ptr = tag.get();
	idx_t len = size();

	data_t is_only_zero = 0;
	for (idx_t i = 0; i < len; ++i) {
		is_only_zero |= tag_ptr[i];
	}
	return is_only_zero == 0;
}

void EncryptionTag::SetSize(idx_t size) {
	tag_len = size;
}

EncryptionCanary::EncryptionCanary() : canary(new data_t[MainHeader::CANARY_BYTE_SIZE]()) {
#ifdef DEBUG
	// Check whether canary is zero-initialized
	for (idx_t i = 0; i < MainHeader::CANARY_BYTE_SIZE; i++) {
		if (canary[i] != 0) {
			throw InvalidInputException("Nonce is not correctly zero-initialized!");
		}
	}
#endif
}

data_ptr_t EncryptionCanary::data() {
	return canary.get();
}

idx_t EncryptionCanary::size() const {
	return MainHeader::CANARY_BYTE_SIZE;
}

EncryptionNonce::EncryptionNonce(CipherType cipher_p, Version version_p) : version(version_p), cipher(cipher_p) {
	switch (version) {
	case Version::V0_0:
		// for prior versions
		// nonce len is 16 for both GCM and CTR
		nonce_len = MainHeader::AES_NONCE_LEN_DEPRECATED;
		nonce = unique_ptr<data_t[]>(new data_t[nonce_len]());
		break;
	case Version::V0_1:
		if (cipher == CipherType::CTR) {
			// for CTR we need a 16-byte nonce / iv
			// the last 4 bytes (counter) are zeroed-out
			nonce_len = MainHeader::AES_IV_LEN;
			nonce = unique_ptr<data_t[]>(new data_t[nonce_len]());
			break;
		}
		// we fall through to NONE (often Parquet) with a 12-byte nonce
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case Version::NONE:
		nonce_len = MainHeader::AES_NONCE_LEN;
		nonce = unique_ptr<data_t[]>(new data_t[nonce_len]());
		break;
	default:
		throw InvalidConfigurationException("Encryption version not recognized!");
	}

#ifdef DEBUG
	// Check whether the nonce is zero-initialized
	for (idx_t i = 0; i < nonce_len; i++) {
		if (nonce[i] != 0) {
			throw InvalidInputException("Nonce is not correctly zero-initialized!");
		}
	}
#endif
}

data_ptr_t EncryptionNonce::data() {
	return nonce.get();
}

idx_t EncryptionNonce::size() const {
	// always return 12 bytes
	if (version == Version::V0_0) {
		// in the first version, nonce was always 16
		return MainHeader::AES_NONCE_LEN_DEPRECATED;
	}
	// in v1, nonce is always 12
	return MainHeader::AES_NONCE_LEN;
}

idx_t EncryptionNonce::total_size() const {
	return nonce_len;
}

idx_t EncryptionNonce::size_deprecated() const {
	return MainHeader::AES_NONCE_LEN_DEPRECATED;
}

void EncryptionNonce::SetSize(idx_t length) {
	nonce_len = length;
}

} // namespace duckdb
