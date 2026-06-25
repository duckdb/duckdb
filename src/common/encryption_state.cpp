#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

idx_t CryptoHash::GetDigestSize(CryptoHashFunction function) {
	switch (function) {
	case CryptoHashFunction::MD5:
		return 16;
	case CryptoHashFunction::SHA1:
		return 20;
	case CryptoHashFunction::SHA256:
		return 32;
	default:
		throw InternalException("Unsupported crypto hash function");
	}
}

idx_t CryptoHash::GetHexDigestSize(CryptoHashFunction function) {
	return GetDigestSize(function) * 2;
}

void CryptoHash::ToHex(const_data_ptr_t input, idx_t input_len, char *output) {
	static constexpr char HEX_CODES[] = "0123456789abcdef";
	for (idx_t input_idx = 0, output_idx = 0; input_idx < input_len; input_idx++) {
		auto byte = input[input_idx];
		output[output_idx++] = HEX_CODES[(byte >> 4) & 0xf];
		output[output_idx++] = HEX_CODES[byte & 0xf];
	}
}

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

void EncryptionUtil::Hash(CryptoHashFunction, const_data_ptr_t, idx_t, data_ptr_t) const {
	throw NotImplementedException("EncryptionUtil does not implement hashing");
}

void EncryptionUtil::HashHex(CryptoHashFunction function, const_data_ptr_t input, idx_t input_len, char *output) const {
	auto digest_size = CryptoHash::GetDigestSize(function);
	data_t digest[CryptoHash::MAX_DIGEST_SIZE];
	D_ASSERT(digest_size <= CryptoHash::MAX_DIGEST_SIZE);
	Hash(function, input, input_len, digest);
	CryptoHash::ToHex(digest, digest_size, output);
}

void EncryptionUtil::Hmac(CryptoHashFunction, const_data_ptr_t, idx_t, const_data_ptr_t, idx_t, data_ptr_t) const {
	throw NotImplementedException("EncryptionUtil does not implement HMAC");
}

bool EncryptionUtil::SupportsHash(CryptoHashFunction) const {
	return false;
}

bool EncryptionUtil::SupportsHmac(CryptoHashFunction) const {
	return false;
}

} // namespace duckdb
