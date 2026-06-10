//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/feature_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"

namespace duckdb {

//! A feature catalog entry
class FeatureCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::FEATURE_ENTRY;
	static constexpr const char *Name = "feature";

public:
	FeatureCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateFeatureInfo &info);

	//! The source table name
	string source_table;
	//! The entity column
	string entity_column;
	//! The timestamp column
	string timestamp_column;
	//! The granularity (DAY/HOUR/MINUTE)
	FeatureGranularity granularity;
	//! The window size
	int64_t window_size;
	//! The refresh mode (FULL/INCREMENTAL)
	FeatureRefreshMode refresh_mode;
	//! The number of versions to retain
	int64_t retain_versions;
	//! The feature query
	unique_ptr<SelectStatement> query;
	//! Current version number (incremented on each REFRESH)
	int64_t current_version;
	//! Timestamp of last refresh (set at creation, derived from backing table at runtime)
	timestamp_t last_refresh_timestamp;

public:
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
	unique_ptr<CreateInfo> GetInfo() const override;
	string ToSQL() const override;
};

} // namespace duckdb
