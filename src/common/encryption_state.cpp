#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

EncryptionState::EncryptionState(unique_ptr<EncryptionStateMetadata> metadata_p) : metadata(std::move(metadata_p)) {
}

EncryptionState::~EncryptionState() {
}

void EncryptionState::InitializeEncryption(EncryptionNonce &nonce, const_data_ptr_t, const_data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::InitializeDecryption(EncryptionNonce &nonce, const_data_ptr_t, const_data_ptr_t, idx_t) {
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

} // namespace duckdb
