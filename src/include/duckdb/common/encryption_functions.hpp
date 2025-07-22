#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/encryption_key_manager.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#endif

namespace duckdb {

struct EncryptionTag {
	EncryptionTag() = default;

	data_ptr_t data() {
		return tag;
	}

	idx_t size() const {
		return MainHeader::AES_TAG_LEN;
	}

private:
	data_t tag[MainHeader::AES_TAG_LEN];
};

struct EncryptionNonce {
	EncryptionNonce() = default;

	data_ptr_t data() {
		return nonce;
	}

	idx_t size() const {
		return MainHeader::AES_NONCE_LEN;
	}

private:
	data_t nonce[MainHeader::AES_NONCE_LEN];
};

class EncryptionEngine {

public:
	EncryptionEngine();
	~EncryptionEngine();

public:
	//! General key management wrapper functions
	static const_data_ptr_t GetKeyFromCache(DatabaseInstance &db, const string &key_name);
	static bool ContainsKey(DatabaseInstance &db, const string &key_name);
	static void AddKeyToCache(DatabaseInstance &db, data_ptr_t key, const string &key_name, bool wipe = true);
	static string AddKeyToCache(DatabaseInstance &db, data_ptr_t key);
	static void AddTempKeyToCache(DatabaseInstance &db);

	//! Encryption Functions
	static void EncryptBlock(DatabaseInstance &db, const string &key_id, FileBuffer &block,
	                         FileBuffer &temp_buffer_manager, uint64_t delta);
	static void DecryptBlock(DatabaseInstance &db, const string &key_id, data_ptr_t internal_buffer,
	                         uint64_t block_size, uint64_t delta);

	static void EncryptTemporaryBuffer(DatabaseInstance &db, data_ptr_t buffer, idx_t buffer_size, data_ptr_t metadata);
	static void DecryptBuffer(EncryptionState &encryption_state, const_data_ptr_t temp_key, data_ptr_t buffer,
	                          idx_t buffer_size, data_ptr_t metadata);
	static void DecryptTemporaryBuffer(DatabaseInstance &db, data_ptr_t buffer, idx_t buffer_size, data_ptr_t metadata);
};

class EncryptionTypes {

public:
	enum CipherType : uint8_t { UNKNOWN = 0, GCM = 1, CTR = 2, CBC = 3 };
	enum KeyDerivationFunction : uint8_t { DEFAULT = 0, SHA256 = 1, PBKDF2 = 2 };

	string CipherToString(CipherType cipher_p) const {
		switch (cipher_p) {
		case GCM:
			return "gcm";
		case CTR:
			return "ctr";
		case CBC:
			return "cbc";
		default:
			return "unknown";
		}
	}

	static CipherType StringToCipher(const string &encryption_cipher) {
		if (encryption_cipher == "gcm") {
			return CipherType::GCM;
		} else if (encryption_cipher == "ctr") {
			return CipherType::CTR;
		} else if (encryption_cipher == "cbc") {
			return CipherType::CBC;
		}
		return CipherType::UNKNOWN;
	}

	string KDFToString(KeyDerivationFunction kdf_p) const {
		switch (kdf_p) {
		case SHA256:
			return "sha256";
		case PBKDF2:
			return "pbkdf2";
		default:
			return "default";
		}
	}

	KeyDerivationFunction StringToKDF(const string &key_derivation_function) const {
		if (key_derivation_function == "sha256") {
			return KeyDerivationFunction::SHA256;
		} else if (key_derivation_function == "pbkdf2") {
			return KeyDerivationFunction::PBKDF2;
		} else {
			return KeyDerivationFunction::DEFAULT;
		}
	}
};

} // namespace duckdb
