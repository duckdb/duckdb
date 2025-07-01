#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

EncryptionState::EncryptionState(const_data_ptr_t key, idx_t key_len) {
	// abstract class, no implementation needed
}

EncryptionState::~EncryptionState() {
}

void EncryptionState::InitializeEncryption(const_data_ptr_t iv, idx_t iv_len, const_data_ptr_t key, idx_t key_len,
                                           const_data_ptr_t aad, idx_t aad_len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::InitializeDecryption(const_data_ptr_t iv, idx_t iv_len, const_data_ptr_t key, idx_t key_len,
                                           const_data_ptr_t aad, idx_t aad_len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

size_t EncryptionState::Process(const_data_ptr_t in, idx_t in_len, data_ptr_t out, idx_t out_len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

size_t EncryptionState::Finalize(data_ptr_t out, idx_t out_len, data_ptr_t tag, idx_t tag_len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::GenerateRandomData(data_ptr_t data, idx_t len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

} // namespace duckdb
