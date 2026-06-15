//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_feature_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Alter Feature
//===--------------------------------------------------------------------===//
//! The kind of ALTER FEATURE operation. A single AlterFeatureInfo is tagged with one of these so
//! that additional feature alters (e.g. schedule changes) can be added without introducing parallel
//! info structs.
enum class AlterFeatureType : uint8_t {
	INVALID = 0,
	//! Bump the feature's current_version (used internally by REFRESH FEATURE)
	BUMP_VERSION = 1,
};

//! Updates a feature transactionally so the change is written to the WAL / checkpoint and survives a
//! restart. The specific operation is selected by alter_feature_type.
struct AlterFeatureInfo : public AlterInfo {
	//! Construct a BUMP_VERSION alter (used internally by REFRESH FEATURE)
	AlterFeatureInfo(AlterEntryData data, int64_t new_version);
	~AlterFeatureInfo() override;

	//! Which kind of feature alter this is
	AlterFeatureType alter_feature_type;
	//! The new current_version to set on the feature (valid when alter_feature_type == BUMP_VERSION)
	int64_t new_version;

public:
	CatalogType GetCatalogType() const override;
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);

	explicit AlterFeatureInfo();
};

} // namespace duckdb
