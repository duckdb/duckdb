//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_crypto.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "parquet_types.h"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/object_cache.hpp"
#endif

namespace duckdb {

using duckdb_apache::thrift::TBase;
using duckdb_apache::thrift::protocol::TProtocol;

class BufferedFileWriter;

class ParquetKeys : public ObjectCacheEntry {
public:
	static ParquetKeys &Get(ClientContext &context);

public:
	void AddKey(const string &key_name, const string &key);
	bool HasKey(const string &key_name) const;
	const string &GetKey(const string &key_name) const;

public:
	static string ObjectType();
	string GetObjectType() override;

private:
	unordered_map<string, string> keys;
};

class ParquetEncryptionConfig {
public:
	explicit ParquetEncryptionConfig(ClientContext &context);
	ParquetEncryptionConfig(ClientContext &context, const Value &arg);

public:
	static shared_ptr<ParquetEncryptionConfig> Create(ClientContext &context, const Value &arg);
	const string &GetFooterKey() const;

public:
	void Serialize(Serializer &serializer) const;
	static shared_ptr<ParquetEncryptionConfig> Deserialize(Deserializer &deserializer);

private:
	ClientContext &context;
	//! Name of the key used for the footer
	string footer_key;
	//! Mapping from column name to key name
	unordered_map<string, string> column_keys;
};

class ParquetCrypto {
public:
	//! Encrypted modules
	static constexpr uint32_t LENGTH_BYTES = 4;
	static constexpr uint32_t NONCE_BYTES = 12;
	static constexpr uint32_t TAG_BYTES = 16;

	//! Block size we encrypt/decrypt
	static constexpr uint32_t CRYPTO_BLOCK_SIZE = 4096;

public:
	//! Decrypt and read a Thrift object from the transport protocol
	static uint32_t Read(TBase &object, TProtocol &iprot, const string &key);
	//! Encrypt and write a Thrift object to the transport protocol
	static uint32_t Write(const TBase &object, TProtocol &oprot, const string &key);
	//! Decrypt and read a buffer
	static uint32_t ReadData(TProtocol &iprot, const data_ptr_t buffer, const uint32_t buffer_size, const string &key);
	//! Encrypt and write a buffer to a file
	static uint32_t WriteData(TProtocol &oprot, const const_data_ptr_t buffer, const uint32_t buffer_size,
	                          const string &key);

public:
	static void AddKey(ClientContext &context, const FunctionParameters &parameters);
};

} // namespace duckdb
