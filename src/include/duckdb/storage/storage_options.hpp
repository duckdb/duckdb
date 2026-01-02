//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

enum class CompressInMemory { AUTOMATIC, COMPRESS, DO_NOT_COMPRESS };

struct StorageOptions {
	//! The allocation size of blocks for this attached database file (if any)
	optional_idx block_alloc_size;
	//! The row group size for this attached database (if any)
	optional_idx row_group_size;
	//! Target storage version (if any)
	optional_idx storage_version;
	//! Block header size (only used for encryption)
	optional_idx block_header_size;

	CompressInMemory compress_in_memory = CompressInMemory::AUTOMATIC;

	//! Whether the database is encrypted
	bool encryption = false;
	//! Encryption algorithm
	EncryptionTypes::CipherType encryption_cipher = EncryptionTypes::INVALID;
	//! encryption key
	//! FIXME: change to a unique_ptr in the future
	shared_ptr<string> user_key;

	void Initialize(const unordered_map<string, Value> &options);
};

} // namespace duckdb
