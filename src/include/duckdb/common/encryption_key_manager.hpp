//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/encryption_key_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

class EncryptionKey {
public:
	explicit EncryptionKey(data_ptr_t encryption_key);
	~EncryptionKey();

	EncryptionKey(const EncryptionKey &) = delete;
	EncryptionKey &operator=(const EncryptionKey &) = delete;

	EncryptionKey(EncryptionKey &&) noexcept = default;
	EncryptionKey &operator=(EncryptionKey &&) noexcept = default;

public:
	const_data_ptr_t GetPtr() const {
		return key;
	}

public:
	static void LockEncryptionKey(data_ptr_t key, idx_t key_len = MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);
	static void UnlockEncryptionKey(data_ptr_t key, idx_t key_len = MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH);

private:
	data_t key[MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH];
};

class EncryptionKeyManager : public ObjectCacheEntry {
public:
	static EncryptionKeyManager &GetInternal(ObjectCache &cache);
	static EncryptionKeyManager &Get(ClientContext &context);
	static EncryptionKeyManager &Get(DatabaseInstance &db);

public:
	void AddKey(const string &key_name, data_ptr_t key);
	bool HasKey(const string &key_name) const;
	void DeleteKey(const string &key_name);
	const_data_ptr_t GetKey(const string &key_name) const;

public:
	static string ObjectType();
	string GetObjectType() override;

public:
public:
	static void DeriveKey(string &user_key, data_ptr_t salt, data_ptr_t derived_key);
	static void KeyDerivationFunctionSHA256(const_data_ptr_t user_key, idx_t user_key_size, data_ptr_t salt,
	                                        data_ptr_t derived_key);
	static void KeyDerivationFunctionSHA256(data_ptr_t user_key, idx_t user_key_size, data_ptr_t salt,
	                                        data_ptr_t derived_key);
	static string Base64Decode(const string &key);

	//! Generate a (non-cryptographically secure) random key ID
	static string GenerateRandomKeyID();

public:
	//! constants
	static constexpr idx_t KEY_ID_BYTES = 8;
	static constexpr idx_t DERIVED_KEY_LENGTH = 32;

private:
	std::unordered_map<std::string, EncryptionKey> derived_keys;
};

} // namespace duckdb
