//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/storage_compatibility.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class AttachedDatabase;

class StorageCompatibility {
public:
	static StorageCompatibility FromDatabase(AttachedDatabase &db);
	static StorageCompatibility FromIndex(idx_t storage_version);
	static StorageCompatibility FromString(const string &input);
	static StorageCompatibility Default();
	static StorageCompatibility Latest();

public:
	bool Compare(idx_t property_version) const;

public:
	//! The user provided version
	string duckdb_version;
	//! The max storage version that should be serialized
	StorageVersion storage_version;
	//! Whether this was set by a manual SET/PRAGMA or default
	bool manually_set;

protected:
	StorageCompatibility() = default;
};

} // namespace duckdb
