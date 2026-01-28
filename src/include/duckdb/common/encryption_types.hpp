//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_types.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/common.hpp"
#include "duckdb/common/string_util.hpp"

#pragma once

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

} // namespace duckdb
