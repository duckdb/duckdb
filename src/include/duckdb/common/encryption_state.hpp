//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

class EncryptionTypes {
public:
	enum CipherType : uint8_t { INVALID = 0, GCM = 1, CTR = 2, CBC = 3 };
	enum KeyDerivationFunction : uint8_t { DEFAULT = 0, SHA256 = 1, PBKDF2 = 2 };
	enum Mode { ENCRYPT, DECRYPT };
	enum EncryptionVersion : uint8_t { V0_0 = 0, V0_1 = 1, NONE = 127 };

	static string CipherToString(CipherType cipher_p);
	static CipherType StringToCipher(const string &encryption_cipher_p);
	static string KDFToString(KeyDerivationFunction kdf_p);
	static KeyDerivationFunction StringToKDF(const string &key_derivation_function_p);
	static EncryptionVersion StringToVersion(const string &encryption_version_p);
};

struct EncryptionStateMetadata {
private:
	const EncryptionTypes::CipherType cipher;
	const idx_t key_len;
	const EncryptionTypes::EncryptionVersion version;

public:
	EncryptionStateMetadata() : cipher(EncryptionTypes::INVALID), key_len(0), version(EncryptionTypes::NONE) {
	}

	EncryptionStateMetadata(EncryptionTypes::CipherType cipher_p, idx_t key_len_p,
	                        EncryptionTypes::EncryptionVersion version_p)
	    : cipher(cipher_p), key_len(key_len_p), version(version_p) {
	}

	bool IsValid() const {
		if (!(cipher != EncryptionTypes::INVALID && key_len > 0)) {
			throw InvalidInputException("EncryptionStateMetadata is not initialized!");
		}
		return true;
	}

	EncryptionTypes::CipherType GetCipher() const {
		return cipher;
	}

	idx_t GetKeyLen() const {
		return key_len;
	}

	EncryptionTypes::EncryptionVersion GetVersion() const {
		return version;
	}
};

class EncryptionState {
public:
	DUCKDB_API explicit EncryptionState(unique_ptr<EncryptionStateMetadata> &metadata);
	DUCKDB_API virtual ~EncryptionState();

public:
	DUCKDB_API virtual void InitializeEncryption(const_data_ptr_t iv, idx_t iv_len, const_data_ptr_t key,
	                                             const_data_ptr_t aad = nullptr, idx_t aad_len = 0);
	DUCKDB_API virtual void InitializeDecryption(const_data_ptr_t iv, idx_t iv_len, const_data_ptr_t key,
	                                             const_data_ptr_t aad = nullptr, idx_t aad_len = 0);
	DUCKDB_API virtual size_t Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len);
	DUCKDB_API virtual size_t Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len);
	DUCKDB_API virtual void GenerateRandomData(data_ptr_t data, idx_t len);

public:
	unique_ptr<EncryptionStateMetadata> &metadata;
};

class EncryptionUtil {
public:
	DUCKDB_API explicit EncryptionUtil() {};

public:
	virtual shared_ptr<EncryptionState> CreateEncryptionState(unique_ptr<EncryptionStateMetadata> &metadata) const {
		return make_shared_ptr<EncryptionState>(metadata);
	}

	virtual ~EncryptionUtil() {
	}

	DUCKDB_API virtual void OverrideEncryptionUtil() {
		throw InvalidInputException("Abstract Method");
	}
};

} // namespace duckdb
