#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

EncryptionState::EncryptionState(EncryptionTypes::CipherType cipher_p, idx_t key_len_p, string aad)
    : cipher(cipher_p), key_len(key_len_p) {
}

EncryptionState::~EncryptionState() {
}

void EncryptionState::InitializeEncryption(const_data_ptr_t, idx_t, const_data_ptr_t, idx_t, const_data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::InitializeDecryption(const_data_ptr_t, idx_t, const_data_ptr_t, idx_t, const_data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

size_t EncryptionState::Process(const_data_ptr_t, idx_t, data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

size_t EncryptionState::Finalize(data_ptr_t, idx_t, data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::GenerateRandomData(data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

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

} // namespace duckdb
