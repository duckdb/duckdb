//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"

namespace duckdb {

class EncryptionTypes {

public:
	enum CipherType : uint8_t { UNKNOWN = 0, GCM = 1, CTR = 2, CBC = 3 };
	enum KeyDerivationFunction : uint8_t { DEFAULT = 0, SHA256 = 1, PBKDF2 = 2 };

	static string CipherToString(CipherType cipher_p) {
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

	static string KDFToString(KeyDerivationFunction kdf_p) {
		switch (kdf_p) {
		case SHA256:
			return "sha256";
		case PBKDF2:
			return "pbkdf2";
		default:
			return "default";
		}
	}

	static KeyDerivationFunction StringToKDF(const string &key_derivation_function) {
		if (key_derivation_function == "sha256") {
			return KeyDerivationFunction::SHA256;
		} else if (key_derivation_function == "pbkdf2") {
			return KeyDerivationFunction::PBKDF2;
		} else {
			return KeyDerivationFunction::DEFAULT;
		}
	}
};

class EncryptionState {

public:
	DUCKDB_API explicit EncryptionState(EncryptionTypes::CipherType cipher_p, const_data_ptr_t key = nullptr,
	                                    idx_t key_len = 0);
	DUCKDB_API virtual ~EncryptionState();

public:
	DUCKDB_API virtual void InitializeEncryption(const_data_ptr_t iv, idx_t iv_len, const_data_ptr_t key, idx_t key_len,
	                                             const_data_ptr_t aad = nullptr, idx_t aad_len = 0);
	DUCKDB_API virtual void InitializeDecryption(const_data_ptr_t iv, idx_t iv_len, const_data_ptr_t key, idx_t key_len,
	                                             const_data_ptr_t aad = nullptr, idx_t aad_len = 0);
	DUCKDB_API virtual size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len);
	DUCKDB_API virtual size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len);
	DUCKDB_API virtual void GenerateRandomData(data_ptr_t data, idx_t len);
};

class EncryptionUtil {

public:
	DUCKDB_API explicit EncryptionUtil() {};

public:
	virtual shared_ptr<EncryptionState> CreateEncryptionState(EncryptionTypes::CipherType cipher_p,
	                                                          const_data_ptr_t key = nullptr, idx_t key_len = 0) const {
		return make_shared_ptr<EncryptionState>(cipher_p, key, key_len);
	}

	virtual ~EncryptionUtil() {
	}
};

} // namespace duckdb
