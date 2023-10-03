//===----------------------------------------------------------------------===//
//                         DuckDB
//
// crypto_wrapper.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "parquet_types.h"

namespace duckdb {

using duckdb_apache::thrift::TBase;
using duckdb_apache::thrift::protocol::TProtocol;

class BufferedFileWriter;

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
};

} // namespace duckdb
