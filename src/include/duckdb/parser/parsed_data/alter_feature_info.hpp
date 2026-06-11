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
//! Updates a feature's current_version transactionally so the new version is written to the
//! WAL / checkpoint and survives a restart.
struct AlterFeatureInfo : public AlterInfo {
	AlterFeatureInfo(AlterEntryData data, int64_t new_version);
	~AlterFeatureInfo() override;

	//! The new current_version to set on the feature
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
