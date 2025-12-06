#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/encryption_key_manager.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/attached_database.hpp"
#include "mbedtls_wrapper.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

EncryptionTag::EncryptionTag() : tag(new data_t[MainHeader::AES_TAG_LEN]) {
}

data_ptr_t EncryptionTag::data() {
	return tag.get();
}

idx_t EncryptionTag::size() const {
	return MainHeader::AES_TAG_LEN;
}

EncryptionNonce::EncryptionNonce() : nonce(new data_t[MainHeader::AES_NONCE_LEN]) {
}

data_ptr_t EncryptionNonce::data() {
	return nonce.get();
}

idx_t EncryptionNonce::size() const {
	return MainHeader::AES_NONCE_LEN;
}

constexpr uint32_t AdditionalAuthenticatedData::INITIAL_AAD_CAPACITY;

AdditionalAuthenticatedData::~AdditionalAuthenticatedData() = default;

data_ptr_t AdditionalAuthenticatedData::data() {
	return additional_authenticated_data->GetData();
}

idx_t AdditionalAuthenticatedData::size() {
	return additional_authenticated_data->GetPosition();
}

void AdditionalAuthenticatedData::WriteData(const_data_ptr_t source, idx_t write_size) {
	additional_authenticated_data->WriteData(source, write_size);
}

EncryptionEngine::EncryptionEngine() {
}

const_data_ptr_t EncryptionEngine::GetKeyFromCache(DatabaseInstance &db, const string &key_name) {
	auto &keys = EncryptionKeyManager::Get(db);
	return keys.GetKey(key_name);
}

bool EncryptionEngine::ContainsKey(DatabaseInstance &db, const string &key_name) {
	auto &keys = EncryptionKeyManager::Get(db);
	return keys.HasKey(key_name);
}

void EncryptionEngine::AddKeyToCache(DatabaseInstance &db, data_ptr_t key, const string &key_name, bool wipe) {
	auto &keys = EncryptionKeyManager::Get(db);
	if (!keys.HasKey(key_name)) {
		keys.AddKey(key_name, key);
	} else {
		// wipe out the key
		std::memset(key, 0, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	}
}

string EncryptionEngine::AddKeyToCache(DatabaseInstance &db, data_ptr_t key) {
	auto &keys = EncryptionKeyManager::Get(db);
	const auto key_id = keys.GenerateRandomKeyID();

	if (!keys.HasKey(key_id)) {
		keys.AddKey(key_id, key);
	} else {
		// wipe out the original key
		std::memset(key, 0, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	}

	return key_id;
}

void EncryptionEngine::AddTempKeyToCache(DatabaseInstance &db) {
	//! Add a temporary key to the cache
	const auto length = MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH;
	data_t temp_key[length];

	auto encryption_state = db.GetEncryptionUtil()->CreateEncryptionState(
	    /* only for random generator */ EncryptionTypes::GCM, length);
	encryption_state->GenerateRandomData(temp_key, length);

	string key_id = "temp_key";
	AddKeyToCache(db, temp_key, key_id);
}

void EncryptionEngine::EncryptBlock(AttachedDatabase &attached_db, const string &key_id, FileBuffer &block,
                                    FileBuffer &temp_buffer_manager, uint64_t delta) {
	auto &db = attached_db.GetDatabase();
	data_ptr_t block_offset_internal = temp_buffer_manager.InternalBuffer();
	auto encrypt_key = GetKeyFromCache(db, key_id);
	auto encryption_state = db.GetEncryptionUtil()->CreateEncryptionState(attached_db.GetStorageManager().GetCipher(),
	                                                                      MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	EncryptionTag tag;
	EncryptionNonce nonce;
	encryption_state->GenerateRandomData(nonce.data(), nonce.size());

	//! store the nonce at the start of the block
	memcpy(block_offset_internal, nonce.data(), nonce.size());
	encryption_state->InitializeEncryption(nonce.data(), nonce.size(), encrypt_key,
	                                       MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	auto checksum_offset = block.InternalBuffer() + delta;
	auto encryption_checksum_offset = block_offset_internal + delta;
	auto size = block.size + Storage::DEFAULT_BLOCK_HEADER_SIZE;

	//! encrypt the data including the checksum
	auto aes_res = encryption_state->Process(checksum_offset, size, encryption_checksum_offset, size);

	if (aes_res != size) {
		throw IOException("Block encryption failure: in- and output size differ (%llu/%llu)", size, aes_res);
	}

	//! Finalize and extract the tag
	encryption_state->Finalize(block.InternalBuffer() + delta, 0, tag.data(), tag.size());

	//! store the generated tag *behind* the nonce (but still at the beginning of the block)
	memcpy(block_offset_internal + nonce.size(), tag.data(), tag.size());
}

void EncryptionEngine::DecryptBlock(AttachedDatabase &attached_db, const string &key_id, data_ptr_t internal_buffer,
                                    uint64_t block_size, uint64_t delta) {
	//! initialize encryption state
	auto &db = attached_db.GetDatabase();

	auto decrypt_key = GetKeyFromCache(db, key_id);
	auto encryption_state = db.GetEncryptionUtil()->CreateEncryptionState(attached_db.GetStorageManager().GetCipher(),
	                                                                      MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	//! load the stored nonce and tag
	EncryptionTag tag;
	EncryptionNonce nonce;
	memcpy(nonce.data(), internal_buffer, nonce.size());
	memcpy(tag.data(), internal_buffer + nonce.size(), tag.size());

	//! Initialize the decryption
	encryption_state->InitializeDecryption(nonce.data(), nonce.size(), decrypt_key,
	                                       MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	auto checksum_offset = internal_buffer + delta;
	auto size = block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE;

	//! decrypt the block including the checksum
	auto aes_res = encryption_state->Process(checksum_offset, size, checksum_offset, size);

	if (aes_res != block_size + Storage::DEFAULT_BLOCK_HEADER_SIZE) {
		throw IOException("Block decryption failure: in- and output size differ (%llu/%llu)", size, aes_res);
	}

	//! check the tag
	encryption_state->Finalize(internal_buffer + delta, 0, tag.data(), tag.size());
}

void EncryptionEngine::EncryptTemporaryBuffer(DatabaseInstance &db, data_ptr_t buffer, idx_t buffer_size,
                                              data_ptr_t metadata) {
	if (!ContainsKey(db, "temp_key")) {
		AddTempKeyToCache(db);
	}

	auto temp_key = GetKeyFromCache(db, "temp_key");

	auto encryption_util = db.GetEncryptionUtil();
	// we hard-code GCM here for now, it's the safest and we don't know what is configured here
	auto encryption_state =
	    encryption_util->CreateEncryptionState(EncryptionTypes::GCM, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	// zero-out the metadata buffer
	memset(metadata, 0, DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE);

	EncryptionTag tag;
	EncryptionNonce nonce;

	encryption_state->GenerateRandomData(nonce.data(), nonce.size());

	//! store the nonce at the start of metadata buffer
	memcpy(metadata, nonce.data(), nonce.size());

	encryption_state->InitializeEncryption(nonce.data(), nonce.size(), temp_key,
	                                       MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	auto aes_res = encryption_state->Process(buffer, buffer_size, buffer, buffer_size);

	if (aes_res != buffer_size) {
		throw IOException("Temporary buffer encryption failure: in- and output size differ (%llu/%llu)", buffer_size,
		                  aes_res);
	}

	//! Finalize and extract the tag
	encryption_state->Finalize(buffer, 0, tag.data(), tag.size());

	//! store the generated tag after consequetively the nonce
	memcpy(metadata + nonce.size(), tag.data(), tag.size());

	// check if tag is correctly stored
	D_ASSERT(memcmp(tag.data(), metadata + nonce.size(), tag.size()) == 0);
}

static void DecryptBuffer(EncryptionState &encryption_state, const_data_ptr_t temp_key, data_ptr_t buffer,
                          idx_t buffer_size, data_ptr_t metadata) {
	//! load the stored nonce and tag
	EncryptionTag tag;
	EncryptionNonce nonce;
	memcpy(nonce.data(), metadata, nonce.size());
	memcpy(tag.data(), metadata + nonce.size(), tag.size());

	//! Initialize the decryption
	encryption_state.InitializeDecryption(nonce.data(), nonce.size(), temp_key,
	                                      MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	auto aes_res = encryption_state.Process(buffer, buffer_size, buffer, buffer_size);

	if (aes_res != buffer_size) {
		throw IOException("Buffer decryption failure: in- and output size differ (%llu/%llu)", buffer_size, aes_res);
	}

	//! check the tag
	encryption_state.Finalize(buffer, 0, tag.data(), tag.size());
}

void EncryptionEngine::DecryptTemporaryBuffer(DatabaseInstance &db, data_ptr_t buffer, idx_t buffer_size,
                                              data_ptr_t metadata) {
	//! initialize encryption state
	auto encryption_util = db.GetEncryptionUtil();
	auto temp_key = GetKeyFromCache(db, "temp_key");
	auto encryption_state =
	    encryption_util->CreateEncryptionState(EncryptionTypes::GCM, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

	DecryptBuffer(*encryption_state, temp_key, buffer, buffer_size, metadata);
}

} // namespace duckdb
