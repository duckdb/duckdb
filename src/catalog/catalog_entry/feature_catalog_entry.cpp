#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/alter_feature_info.hpp"

namespace duckdb {

FeatureCatalogEntry::FeatureCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateFeatureInfo &info)
    : StandardEntry(CatalogType::FEATURE_ENTRY, schema, catalog, info.feature_name), source_table(info.source_table),
      entity_column(info.entity_column), timestamp_column(info.timestamp_column), granularity(info.granularity),
      window_size(info.window_size), refresh_mode(info.refresh_mode), retain_versions(info.retain_versions),
      current_version(info.current_version), last_refresh_timestamp(Timestamp::GetCurrentTimestamp()) {
	if (info.query) {
		query = unique_ptr_cast<SQLStatement, SelectStatement>(info.query->Copy());
	}
}

unique_ptr<CatalogEntry> FeatureCatalogEntry::Copy(ClientContext &context) const {
	auto info_copy = GetInfo();
	auto &cast_info = info_copy->Cast<CreateFeatureInfo>();
	auto result = make_uniq<FeatureCatalogEntry>(catalog, schema, cast_info);
	return std::move(result);
}

unique_ptr<CatalogEntry> FeatureCatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_FEATURE) {
		throw InternalException("Attempting to alter FeatureCatalogEntry with unsupported alter type");
	}
	auto &feature_info = info.Cast<AlterFeatureInfo>();
	// Produce a new entry that is identical except for the bumped current_version. Going through the
	// catalog (rather than mutating in place) makes the change transactional, so it is written to the
	// WAL / checkpoint and survives a restart.
	auto create_info = GetInfo();
	auto &cast_info = create_info->Cast<CreateFeatureInfo>();
	cast_info.current_version = feature_info.new_version;
	return make_uniq<FeatureCatalogEntry>(catalog, schema, cast_info);
}

unique_ptr<CreateInfo> FeatureCatalogEntry::GetInfo() const {
	auto info = make_uniq<CreateFeatureInfo>();
	info->feature_name = name;
	info->source_table = source_table;
	info->entity_column = entity_column;
	info->timestamp_column = timestamp_column;
	info->granularity = granularity;
	info->window_size = window_size;
	info->refresh_mode = refresh_mode;
	info->retain_versions = retain_versions;
	info->current_version = current_version;
	if (query) {
		info->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	}
	info->schema = schema.name;
	info->catalog = catalog.GetName();
	return std::move(info);
}

string FeatureCatalogEntry::ToSQL() const {
	auto info = GetInfo();
	return info->ToString();
}

} // namespace duckdb
