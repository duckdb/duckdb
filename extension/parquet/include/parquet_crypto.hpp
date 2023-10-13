//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_crypto.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "duckdb/storage/object_cache.hpp"
#include "parquet_types.h"

namespace duckdb {

using duckdb_apache::thrift::TBase;
using duckdb_apache::thrift::protocol::TProtocol;

class BufferedFileWriter;

class ParquetKey : public ObjectCacheEntry {
public:
	explicit ParquetKey(string key_p) : key(std::move(key_p)) {
	}

	string key;

public:
	static string ObjectType() {
		return "parquet_key";
	}

	string GetObjectType() override {
		return ObjectType();
	}
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
	static void SetKey(ClientContext &context, const FunctionParameters &parameters);
	static bool HasKey(ClientContext &context);
	static string GetKey(ClientContext &context);
};

} // namespace duckdb
