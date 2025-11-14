#pragma once

#include "duckdb/common/helper.hpp"

namespace duckdb {

class DatabaseInstance;
class AttachedDatabase;
class FileBuffer;

struct EncryptionTag {
	EncryptionTag();
	data_ptr_t data();
	idx_t size() const;

private:
	unique_ptr<data_t[]> tag;
};

struct EncryptionNonce {
	EncryptionNonce();
	data_ptr_t data();
	idx_t size() const;

private:
	unique_ptr<data_t[]> nonce;
};

class EncryptionEngine {
public:
	EncryptionEngine();
	~EncryptionEngine();

public:
	//! General key management wrapper functions
	static const_data_ptr_t GetKeyFromCache(DatabaseInstance &db, const string &key_name);
	static bool ContainsKey(DatabaseInstance &db, const string &key_name);
	static void AddKeyToCache(DatabaseInstance &db, data_ptr_t key, const string &key_name, bool wipe = true);
	static string AddKeyToCache(DatabaseInstance &db, data_ptr_t key);
	static void AddTempKeyToCache(DatabaseInstance &db);

	//! Encryption Functions
	static void EncryptBlock(AttachedDatabase &attached_db, const string &key_id, FileBuffer &block,
	                         FileBuffer &temp_buffer_manager, uint64_t delta);
	static void DecryptBlock(AttachedDatabase &attached_db, const string &key_id, data_ptr_t internal_buffer,
	                         uint64_t block_size, uint64_t delta);

	static void EncryptTemporaryBuffer(DatabaseInstance &db, data_ptr_t buffer, idx_t buffer_size, data_ptr_t metadata);

	static void DecryptTemporaryBuffer(DatabaseInstance &db, data_ptr_t buffer, idx_t buffer_size, data_ptr_t metadata);
};

} // namespace duckdb
