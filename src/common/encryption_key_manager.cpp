#include "duckdb/common/encryption_key_manager.hpp"
#include "mbedtls_wrapper.hpp"

#if defined(_WIN32)
#include "duckdb/common/windows.hpp"
#else
#include <sys/mman.h>
#undef MAP_TYPE
#endif

namespace duckdb {

EncryptionKey::EncryptionKey(const string &encryption_key_p) : encryption_key(encryption_key_p) {
	// Lock the encryption key
	LockEncryptionKey(encryption_key);
}

// destructor
EncryptionKey::~EncryptionKey() {
	if (!encryption_key.empty()) {
		UnlockEncryptionKey(encryption_key);
	}
}

void EncryptionKey::LockEncryptionKey(string &key) {
	if (!key.empty()) {
#if defined(_WIN32)
		VirtualLock(static_cast<void *>(&key[0]), EncryptionKeyManager::DERIVED_KEY_LENGTH);
#else
		mlock(static_cast<void *>(&key[0]), EncryptionKeyManager::DERIVED_KEY_LENGTH);
#endif
	}
}

void EncryptionKey::UnlockEncryptionKey(string &key) {
	fill(key.begin(), key.end(), 0); // zero out contents
	key.clear();
#if defined(_WIN32)
	VirtualUnlock(static_cast<void *>(&key[0]), EncryptionKeyManager::DERIVED_KEY_LENGTH);
#else
	munlock(static_cast<void *>(&key[0]), EncryptionKeyManager::DERIVED_KEY_LENGTH);
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
	duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLS::GenerateRandomDataStatic(key_id, KEY_ID_BYTES);
	string key_id_str(reinterpret_cast<const char *>(key_id), KEY_ID_BYTES);
	return key_id_str;
}

void EncryptionKeyManager::AddKey(const string &key_name, string &key) {
	derived_keys.emplace(key_name, EncryptionKey(key));
	// wipe out the original key
	std::memset(&key[0], 0, key.size());
	key.clear();
}

bool EncryptionKeyManager::HasKey(const string &key_name) const {
	return derived_keys.find(key_name) != derived_keys.end();
}

const string &EncryptionKeyManager::GetKey(const string &key_name) const {
	D_ASSERT(HasKey(key_name));
	auto &key = derived_keys.at(key_name);
	return key.Get();
}

void EncryptionKeyManager::DeleteKey(const string &key_name) {
	derived_keys.erase(key_name);
}

string EncryptionKeyManager::KeyDerivationFunctionSHA256(const string &user_key, data_ptr_t salt) {
	//! For now, we are only using SHA256 for key derivation
	duckdb_mbedtls::MbedTlsWrapper::SHA256State state;
	state.AddSalt(salt, MainHeader::SALT_LEN);
	state.AddString(user_key);
	auto derived_key = state.Finalize();
	//! key_length is hardcoded to 32 bytes
	D_ASSERT(derived_key.length() == MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	return derived_key;
}

string EncryptionKeyManager::DeriveKey(const string &user_key, data_ptr_t salt) {
	return KeyDerivationFunctionSHA256(user_key, salt);
}

string EncryptionKeyManager::ObjectType() {
	return "encryption_keys";
}

string EncryptionKeyManager::GetObjectType() {
	return ObjectType();
}

} // namespace duckdb
