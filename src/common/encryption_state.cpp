#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

EncryptionState::EncryptionState() {
	// abstract class, no implementation needed
}

EncryptionState::~EncryptionState() {
}

bool EncryptionState::IsOpenSSL() {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::InitializeEncryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, const std::string *key) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::InitializeDecryption(duckdb::const_data_ptr_t iv, duckdb::idx_t iv_len, const std::string *key) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

size_t EncryptionState::Process(duckdb::const_data_ptr_t in, duckdb::idx_t in_len, duckdb::data_ptr_t out,
                                duckdb::idx_t out_len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

size_t EncryptionState::Finalize(duckdb::data_ptr_t out, duckdb::idx_t out_len, duckdb::data_ptr_t tag,
                                 duckdb::idx_t tag_len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

void EncryptionState::GenerateRandomData(duckdb::data_ptr_t data, duckdb::idx_t len) {
	throw NotImplementedException("EncryptionState Abstract Class is called");
}

} // namespace duckdb
