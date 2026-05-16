#include "duckdb/common/storage_compatibility.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

StorageCompatibility StorageCompatibility::FromDatabase(AttachedDatabase &db) {
	return FromIndex(db.GetStorageManager().GetStorageVersion());
}

StorageCompatibility StorageCompatibility::FromIndex(StorageVersion storage_version_p) {
	StorageCompatibility result;

	result.storage_version = storage_version_p;
	result.duckdb_version = StorageVersionInfo::GetStorageVersionString(storage_version_p);
	result.manually_set = false;
	return result;
}

StorageCompatibility StorageCompatibility::FromString(const string &input) {
	if (input.empty()) {
		throw InvalidInputException("Version string can not be empty");
	}

	auto storage_version = GetStorageVersion(input.c_str());
	if (storage_version == StorageVersion::INVALID) {
		auto candidates = GetStorageCandidates();
		throw InvalidInputException("The version string '%s' is not a known DuckDB version, valid options are: %s",
		                            input, StringUtil::Join(candidates, ", "));
	}
	StorageCompatibility result;
	result.duckdb_version = input;
	result.storage_version = storage_version;
	result.manually_set = true;
	return result;
}

StorageCompatibility StorageCompatibility::Default() {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	auto res = FromString("latest");
	res.duckdb_version = "latest";
	res.manually_set = false;
	return res;
#else
#ifdef DUCKDB_LATEST_STORAGE
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
	auto res = FromString("v0.10.2");
	res.duckdb_version = "latest";
	res.manually_set = false;
	return res;
#endif
#endif
}

StorageCompatibility StorageCompatibility::Latest() {
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
}

bool StorageCompatibility::Compare(StorageVersion property_version) const {
	return property_version <= storage_version;
}

bool StorageCompatibility::CompareVersionString(const string &property_version) const {
	auto property_version_val = GetSerializationVersionDeprecated(property_version.c_str());
	auto deprecated_serialization_version = GetSerializationVersionDeprecated(duckdb_version.c_str());
	return property_version_val <= deprecated_serialization_version;
}

StorageVersion StorageCompatibility::GetStorageVersionCompatibility() const {
	return storage_version;
}

} // namespace duckdb
