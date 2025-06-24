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

namespace duckdb {

struct StorageOptions {
	//! The allocation size of blocks for this attached database file (if any)
	optional_idx block_alloc_size;
	//! The row group size for this attached database (if any)
	optional_idx row_group_size;
	//! Target storage version (if any)
	optional_idx storage_version;
	//! Block header size (only used for encryption)
	optional_idx block_header_size;

	//! Whether the database is encrypted
	bool encryption = false;
	//! Encryption algorithm (default = GCM)
	string encryption_cipher = "gcm";
	//! encryption key
	//! FIXME: change to a unique_ptr in the future
	shared_ptr<string> user_key;
};

inline void ClearUserKey(shared_ptr<string> const &encryption_key) {
	if (encryption_key && !encryption_key->empty()) {
		memset(&(*encryption_key)[0], 0, encryption_key->size());
		encryption_key->clear();
	}
}

} // namespace duckdb
