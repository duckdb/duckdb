//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/storage_info.hpp"

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

struct EncryptionTag {
	EncryptionTag();
	data_ptr_t data();
	idx_t size() const;
	bool IsAllZeros() const;
	void SetSize(idx_t size);

private:
	unique_ptr<data_t[]> tag;
	idx_t tag_len;
};

struct EncryptionCanary {
	EncryptionCanary();
	data_ptr_t data();
	idx_t size() const;

private:
	unique_ptr<data_t[]> canary;
};

struct EncryptionNonce {
	explicit EncryptionNonce(EncryptionTypes::CipherType cipher = EncryptionTypes::GCM,
	                         EncryptionTypes::EncryptionVersion version = EncryptionTypes::EncryptionVersion::V0_1);
	data_ptr_t data();
	idx_t size() const;
	idx_t total_size() const;
	idx_t size_deprecated() const;

	void SetSize(idx_t length);

private:
	unique_ptr<data_t[]> nonce;
	idx_t nonce_len;

private:
	EncryptionTypes::EncryptionVersion version;
	EncryptionTypes::CipherType cipher;
};

} // namespace duckdb
