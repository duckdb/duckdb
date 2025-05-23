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

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#endif

namespace duckdb {

class EncryptionKey {

public:
	explicit EncryptionKey(const string &encryption_key);
	~EncryptionKey();

	EncryptionKey(const EncryptionKey &) = delete;
	EncryptionKey &operator=(const EncryptionKey &) = delete;

	EncryptionKey(EncryptionKey &&) noexcept = default;
	EncryptionKey &operator=(EncryptionKey &&) noexcept = default;

public:
	const string &Get() const {
		return encryption_key;
	}

private:
	string encryption_key;

private:
	static void LockEncryptionKey(string &key);
	static void UnlockEncryptionKey(string &key);
};

class EncryptionKeyManager : public ObjectCacheEntry {

public:
	static EncryptionKeyManager &GetInternal(ObjectCache &cache);
	static EncryptionKeyManager &Get(ClientContext &context);
	static EncryptionKeyManager &Get(DatabaseInstance &db);

public:
	void AddKey(const string &key_name, string &key);
	bool HasKey(const string &key_name) const;
	void DeleteKey(const string &key_name);
	const string &GetKey(const string &key_name) const;

public:
	static string ObjectType();
	string GetObjectType() override;

public:
	static string DeriveKey(const string &user_key, data_ptr_t salt);
	static string KeyDerivationFunctionSHA256(const string &user_key, data_ptr_t salt);
	static string GenerateRandomKeyID();

public:
	//! constants
	static constexpr idx_t KEY_ID_BYTES = 8;
	static constexpr idx_t DERIVED_KEY_LENGTH = 32;

private:
	unordered_map<string, EncryptionKey> derived_keys;
};

} // namespace duckdb
