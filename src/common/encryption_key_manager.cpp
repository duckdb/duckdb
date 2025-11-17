#include "duckdb/common/encryption_key_manager.hpp"
#include "mbedtls_wrapper.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/blob.hpp"

#if defined(_WIN32)
#include "duckdb/common/windows.hpp"
#else
#include <sys/mman.h>
#undef MAP_TYPE
#endif

namespace duckdb {

EncryptionKey::EncryptionKey(data_ptr_t encryption_key_p) {
	// Lock the encryption key
	memcpy(key, encryption_key_p, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	D_ASSERT(memcmp(key, encryption_key_p, MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH) == 0);

	// zero out the encryption key in memory
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(encryption_key_p,
	                                                                 MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	LockEncryptionKey(key);
}

// destructor
EncryptionKey::~EncryptionKey() {
	UnlockEncryptionKey(key);
}

void EncryptionKey::LockEncryptionKey(data_ptr_t key, idx_t key_len) {
#if defined(_WIN32)
	VirtualLock(key, key_len);
#elif defined(__MVS__)
	__mlockall(_BPX_NONSWAP);
#else
	mlock(key, key_len);
#endif
}

void EncryptionKey::UnlockEncryptionKey(data_ptr_t key, idx_t key_len) {
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(key, key_len);
#if defined(_WIN32)
	VirtualUnlock(key, key_len);
#elif defined(__MVS__)
	__mlockall(_BPX_SWAP);
#else
	munlock(key, key_len);
#endif
}

EncryptionKeyManager &EncryptionKeyManager::GetInternal(ObjectCache &cache) {
	if (!cache.Get<EncryptionKeyManager>(EncryptionKeyManager::ObjectType())) {
		cache.Put(EncryptionKeyManager::ObjectType(), make_shared_ptr<EncryptionKeyManager>());
	}
	return *cache.Get<EncryptionKeyManager>(EncryptionKeyManager::ObjectType());
}

EncryptionKeyManager &EncryptionKeyManager::Get(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return GetInternal(cache);
}

EncryptionKeyManager &EncryptionKeyManager::Get(DatabaseInstance &db) {
	auto &cache = db.GetObjectCache();
	return GetInternal(cache);
}

string EncryptionKeyManager::GenerateRandomKeyID() {
	uint8_t key_id[KEY_ID_BYTES];
	RandomEngine engine;
	engine.RandomData(key_id, KEY_ID_BYTES);
	string key_id_str(reinterpret_cast<const char *>(key_id), KEY_ID_BYTES);
	return key_id_str;
}

void EncryptionKeyManager::AddKey(const string &key_name, data_ptr_t key) {
	derived_keys.emplace(key_name, EncryptionKey(key));
	// Zero-out the encryption key
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(key, DERIVED_KEY_LENGTH);
}

bool EncryptionKeyManager::HasKey(const string &key_name) const {
	return derived_keys.find(key_name) != derived_keys.end();
}

const_data_ptr_t EncryptionKeyManager::GetKey(const string &key_name) const {
	D_ASSERT(HasKey(key_name));
	return derived_keys.at(key_name).GetPtr();
}

void EncryptionKeyManager::DeleteKey(const string &key_name) {
	derived_keys.erase(key_name);
}

void EncryptionKeyManager::KeyDerivationFunctionSHA256(const_data_ptr_t key, idx_t key_size, data_ptr_t salt,
                                                       data_ptr_t derived_key) {
	//! For now, we are only using SHA256 for key derivation
	duckdb_mbedtls::MbedTlsWrapper::SHA256State state;
	state.AddSalt(salt, MainHeader::DB_IDENTIFIER_LEN);
	state.AddBytes(key, key_size);
	state.FinalizeDerivedKey(derived_key);
}

void EncryptionKeyManager::KeyDerivationFunctionSHA256(data_ptr_t user_key, idx_t user_key_size, data_ptr_t salt,
                                                       data_ptr_t derived_key) {
	KeyDerivationFunctionSHA256(reinterpret_cast<const_data_ptr_t>(user_key), user_key_size, salt, derived_key);
}

string EncryptionKeyManager::Base64Decode(const string &key) {
	auto result_size = Blob::FromBase64Size(key);
	auto output = duckdb::unique_ptr<unsigned char[]>(new unsigned char[result_size]);
	Blob::FromBase64(key, output.get(), result_size);
	string decoded_key(reinterpret_cast<const char *>(output.get()), result_size);
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(output.get(), result_size);
	return decoded_key;
}

void EncryptionKeyManager::DeriveKey(string &user_key, data_ptr_t salt, data_ptr_t derived_key) {
	string decoded_key;

	try {
		//! Key is base64 encoded
		decoded_key = Base64Decode(user_key);
	} catch (const ConversionException &e) {
		//! Todo; check if valid utf-8
		decoded_key = user_key;
	}

	KeyDerivationFunctionSHA256(reinterpret_cast<const_data_ptr_t>(decoded_key.data()), decoded_key.size(), salt,
	                            derived_key);
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(data_ptr_cast(&user_key[0]), user_key.size());
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::SecureClearData(data_ptr_cast(&decoded_key[0]),
	                                                                 decoded_key.size());
	user_key.clear();
	decoded_key.clear();
}

string EncryptionKeyManager::ObjectType() {
	return "encryption_keys";
}

string EncryptionKeyManager::GetObjectType() {
	return ObjectType();
}

} // namespace duckdb
