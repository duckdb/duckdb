//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/storage_compatibility.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class AttachedDatabase;

class StorageCompatibility {
public:
	static StorageCompatibility FromDatabase(AttachedDatabase &db);
	static StorageCompatibility FromIndex(StorageVersion storage_version_p);
	static StorageCompatibility FromString(const string &input);
	static StorageCompatibility Default();
	static StorageCompatibility Latest();

public:
	bool Compare(StorageVersion property_version) const;
	bool CompareVersionString(const string &property_version) const;
	StorageVersion GetStorageVersionCompatibility() const;

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
