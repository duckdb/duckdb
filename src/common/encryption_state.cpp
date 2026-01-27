#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

EncryptionState::EncryptionState(unique_ptr<EncryptionStateMetadata> &metadata_p) : metadata(metadata_p) {
}

EncryptionState::~EncryptionState() {
}

void EncryptionState::InitializeEncryption(const_data_ptr_t, idx_t, const_data_ptr_t, const_data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::InitializeDecryption(const_data_ptr_t, idx_t, const_data_ptr_t, const_data_ptr_t, idx_t) {
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

} // namespace duckdb
