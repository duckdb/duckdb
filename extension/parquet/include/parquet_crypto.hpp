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
	static constexpr uint32_t CRYPTO_BLOCK_SIZE = 2048;

public:
	//! Write a
	static void Read(TBase &object, TProtocol &iprot, const string &key);

	static void Write(const TBase &object, TProtocol &oprot, const string &key);

	static void WriteData(BufferedFileWriter &writer, const const_data_ptr_t buffer, const uint32_t buffer_size,
	                      const string &key);
};

} // namespace duckdb
