#include "duckdb/common/storage_compatibility.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

StorageCompatibility StorageCompatibility::FromDatabase(AttachedDatabase &db) {
	return FromIndex(db.GetStorageManager().GetStorageVersion());
}

StorageCompatibility StorageCompatibility::FromIndex(const idx_t version) {
	StorageCompatibility result;
	result.duckdb_version = "";
	result.storage_version = version;
	result.manually_set = false;
	return result;
}

StorageCompatibility StorageCompatibility::FromString(const string &input) {
	if (input.empty()) {
		throw InvalidInputException("Version string can not be empty");
	}

	auto storage_version = GetSerializationVersion(input.c_str());
	if (!storage_version.IsValid()) {
		auto candidates = GetSerializationCandidates();
		throw InvalidInputException("The version string '%s' is not a known DuckDB version, valid options are: %s",
		                            input, StringUtil::Join(candidates, ", "));
	}
	StorageCompatibility result;
	result.duckdb_version = input;
	result.storage_version = storage_version.GetIndex();
	result.manually_set = true;
	return result;
}

StorageCompatibility StorageCompatibility::Default() {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
#ifdef DUCKDB_LATEST_STORAGE
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
#else
	auto res = FromString("v0.10.2");
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

bool StorageCompatibility::Compare(idx_t property_version) const {
	return property_version <= storage_version;
}

} // namespace duckdb
