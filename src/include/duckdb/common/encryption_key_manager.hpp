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

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#endif

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

class MasterKey {

public:
	explicit MasterKey(data_ptr_t input_key, idx_t input_size) {
		// A master key can be of variable size
		master_key = new data_t[input_size];
		memcpy(master_key, input_key, input_size);
		key_size = input_size;

		EncryptionKey::LockEncryptionKey(master_key, key_size);
	}

	~MasterKey() {
		EncryptionKey::UnlockEncryptionKey(master_key, key_size);
		delete[] master_key;
		master_key = nullptr;
		key_size = 0;
	};

	MasterKey(const MasterKey &) = delete;
	MasterKey &operator=(const MasterKey &) = delete;

	MasterKey(MasterKey &&) noexcept = default;
	MasterKey &operator=(MasterKey &&) noexcept = default;

public:
	const_data_ptr_t GetPtr() const {
		return master_key;
	}

	idx_t GetSize() const {
		return key_size;
	}

private:
	data_ptr_t master_key;
	idx_t key_size;
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
	void SetMasterKey(data_ptr_t input_key, idx_t input_size) {
		master_key_initialized = true;
		master_key = make_uniq<MasterKey>(input_key, input_size);
	}

	void ClearMasterKey() {
		master_key = nullptr;
		master_key_initialized = false;
	}

	bool HasMasterKey() const {
		return master_key_initialized;
	}

	const_data_ptr_t GetMasterKey() const {
		return master_key->GetPtr();
	}

	idx_t GetMasterKeySize() const {
		return master_key->GetSize();
	}

public:
	static void DeriveKey(string &user_key, data_ptr_t salt, data_ptr_t derived_key);
	static void DeriveMasterKey(const_data_ptr_t master_key, idx_t key_size, data_ptr_t salt, data_ptr_t derived_key);
	static void KeyDerivationFunctionSHA256(const_data_ptr_t user_key, idx_t user_key_size, data_ptr_t salt,
	                                        data_ptr_t derived_key);
	static void KeyDerivationFunctionSHA256(data_ptr_t user_key, idx_t user_key_size, data_ptr_t salt,
	                                        data_ptr_t derived_key);
	static string Base64Decode(const string &key);
	static string GenerateRandomKeyID();

public:
	//! constants
	static constexpr idx_t KEY_ID_BYTES = 8;
	static constexpr idx_t DERIVED_KEY_LENGTH = 32;

private:
	std::unordered_map<std::string, EncryptionKey> derived_keys;

	shared_ptr<MasterKey> master_key;
	bool master_key_initialized = false;
};

} // namespace duckdb
