//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_crypto.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_types.h"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/storage/object_cache.hpp"

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
	explicit ParquetEncryptionConfig();
	ParquetEncryptionConfig(ClientContext &context, const Value &arg);
	ParquetEncryptionConfig(string footer_key);

public:
	static shared_ptr<ParquetEncryptionConfig> Create(ClientContext &context, const Value &arg);
	const string &GetFooterKey() const;

public:
	void Serialize(Serializer &serializer) const;
	static shared_ptr<ParquetEncryptionConfig> Deserialize(Deserializer &deserializer);

private:
	//! The encryption key used for the footer
	string footer_key;
	//! Mapping from column name to key name
	unordered_map<string, string> column_keys;
};

class ParquetCrypto {
public:
	//! Encrypted modules
	static constexpr idx_t LENGTH_BYTES = 4;
	static constexpr idx_t NONCE_BYTES = 12;
	static constexpr idx_t TAG_BYTES = 16;

	//! Block size we encrypt/decrypt
	static constexpr idx_t CRYPTO_BLOCK_SIZE = 4096;
	static constexpr idx_t BLOCK_SIZE = 16;

	// Module types for encryption
	static constexpr int8_t Footer = 0;
	static constexpr int8_t ColumnMetaData = 1;
	static constexpr int8_t DataPage = 2;
	static constexpr int8_t DictionaryPage = 3;
	static constexpr int8_t DataPageHeader = 4;
	static constexpr int8_t DictionaryPageHeader = 5;
	static constexpr int8_t ColumnIndex = 6;
	static constexpr int8_t OffsetIndex = 7;
	static constexpr int8_t BloomFilterHeader = 8;
	static constexpr int8_t BloomFilterBitset = 9;

	// Standard AAD length for file
	static constexpr int32_t AADFileIDLength = 8;

public:
	//! Decrypt and read a Thrift object from the transport protocol
	static uint32_t Read(TBase &object, TProtocol &iprot, const string &key, const EncryptionUtil &encryption_util_p,
	                     string aad = "");
	//! Encrypt and write a Thrift object to the transport protocol
	static uint32_t Write(const TBase &object, TProtocol &oprot, const string &key,
	                      const EncryptionUtil &encryption_util_p);
	//! Decrypt and read a buffer
	static uint32_t ReadData(TProtocol &iprot, const data_ptr_t buffer, const uint32_t buffer_size, const string &key,
	                         const EncryptionUtil &encryption_util_p, string aad = "");
	//! Encrypt and write a buffer to a file
	static uint32_t WriteData(TProtocol &oprot, const const_data_ptr_t buffer, const uint32_t buffer_size,
	                          const string &key, const EncryptionUtil &encryption_util_p);
	// Create Additional Authenticated Data for each module
	uint8_t *CreateModuleAad(const std::string &file_aad, int8_t module_type, int16_t row_group_ordinal = -1,
	                         int16_t column_ordinal = -1, int16_t page_ordinal = -1);
	// Create Additional Authenticated Data for the footer
	uint8_t *CreateFooterAad(const std::string &aad_prefix_bytes);

public:
	static void AddKey(ClientContext &context, const FunctionParameters &parameters);
	static bool ValidKey(const std::string &key);
};

} // namespace duckdb
