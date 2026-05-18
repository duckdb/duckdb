#include "duckdb/common/serialization_compatibility.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

SerializationCompatibility SerializationCompatibility::FromDatabase(AttachedDatabase &db) {
	return FromIndex(db.GetStorageManager().GetStorageVersion());
}

SerializationCompatibility SerializationCompatibility::FromIndex(const idx_t version) {
	SerializationCompatibility result;
	result.duckdb_version = "";
	result.serialization_version = version;
	result.manually_set = false;
	return result;
}

SerializationCompatibility SerializationCompatibility::FromString(const string &input) {
	if (input.empty()) {
		throw InvalidInputException("Version string can not be empty");
	}

	auto serialization_version = GetSerializationVersion(input.c_str());
	if (!serialization_version.IsValid()) {
		auto candidates = GetSerializationCandidates();
		throw InvalidInputException("The version string '%s' is not a known DuckDB version, valid options are: %s",
		                            input, StringUtil::Join(candidates, ", "));
	}
	SerializationCompatibility result;
	result.duckdb_version = input;
	result.serialization_version = serialization_version.GetIndex();
	result.manually_set = true;
	return result;
}

SerializationCompatibility SerializationCompatibility::Default() {
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

SerializationCompatibility SerializationCompatibility::Latest() {
	auto res = FromString("latest");
	res.manually_set = false;
	return res;
}

bool SerializationCompatibility::Compare(idx_t property_version) const {
	return property_version <= serialization_version;
}

} // namespace duckdb
