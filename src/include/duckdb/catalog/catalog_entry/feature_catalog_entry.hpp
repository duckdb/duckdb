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

	//! The entity table name (one snapshot row per entity); declared via the ENTITY clause
	string entity_table;
	//! The entity columns (entity table primary key names); empty for global features
	vector<string> entity_columns;
	//! The entity table key columns, aligned to entity_columns
	vector<string> entity_key_columns;
	//! The timestamp column
	string timestamp_column;
	//! Optional table qualifier for the timestamp column (empty if the reference is unqualified)
	string timestamp_table;
	//! The lookback window interval
	interval_t window_interval;
	//! TTL / serving staleness bound: snapshots older than this (relative to the request time) serve as NULL.
	//! A zero interval disables the bound.
	interval_t ttl_interval;
	//! The number of versions to retain
	int64_t retain_versions;
	//! The feature query
	unique_ptr<SelectStatement> query;
	//! Current version number (incremented on each REFRESH)
	int64_t current_version;
	//! Timestamp of last refresh (set at creation, derived from backing table at runtime)
	timestamp_t last_refresh_timestamp;
	//! Whether an automatic refresh schedule is attached to this feature
	bool has_schedule;
	//! The interval between automatic refreshes (valid when has_schedule is true)
	interval_t schedule_interval;
	//! Whether the schedule is currently active; can be toggled without dropping the interval
	bool schedule_enabled;

public:
	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;
	unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction transaction, AlterInfo &info) override;
	unique_ptr<CreateInfo> GetInfo() const override;
	string ToSQL() const override;
};

} // namespace duckdb
