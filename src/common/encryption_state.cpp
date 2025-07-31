#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

EncryptionState::EncryptionState(EncryptionTypes::CipherType, const_data_ptr_t, idx_t) {
	// abstract class, no implementation needed
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

void EncryptionState::Finalize(data_ptr_t, idx_t, data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::GenerateRandomData(data_ptr_t, idx_t) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

} // namespace duckdb
