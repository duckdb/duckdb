#include "duckdb/execution/operator/schema/physical_create_feature.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/feature_refresh_scheduler.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &client = context.client;
	auto &catalog = Catalog::GetCatalog(client, info->catalog);
	auto &schema = catalog.GetSchema(client, info->schema);
	auto transaction = catalog.GetCatalogTransaction(client);

	// Check IF NOT EXISTS early: if the feature already exists, do nothing.
	auto &duck_schema = schema.Cast<DuckSchemaEntry>();
	auto &set = duck_schema.GetCatalogSet(CatalogType::FEATURE_ENTRY);
	if (info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto old_entry = set.GetEntry(transaction, info->feature_name);
		if (old_entry) {
			chunk.SetCardinality(1);
			chunk.SetValue(0, 0, Value(info->feature_name));
			return SourceResultType::FINISHED;
		}
	}

	// CREATE FEATURE only registers metadata; no version table is materialized here. The first version
	// table (feature_name__v1) is produced by the first REFRESH FEATURE. Until then current_version is 0,
	// and queries/serves against the feature raise an error asking the user to refresh it first.

	// Create a view named feature_name that resolves via current_feature()
	auto view_info = make_uniq<CreateViewInfo>(info->catalog, info->schema, info->feature_name);
	view_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	auto select_sql = "SELECT * FROM current_feature('" + info->feature_name + "')";
	view_info->query = CreateViewInfo::ParseSelect(select_sql);
	auto view_entry = catalog.CreateView(client, *view_info);

	// Create the feature catalog entry
	auto &dependencies = info->dependencies;
	auto entry = make_uniq<FeatureCatalogEntry>(catalog, schema, *info);

	if (!set.CreateEntry(transaction, info->feature_name, std::move(entry), dependencies)) {
		throw CatalogException::EntryAlreadyExists(CatalogType::FEATURE_ENTRY, info->feature_name);
	}
	if (info->has_schedule && info->schedule_enabled) {
		if (auto scheduler = catalog.GetDatabase().GetFeatureRefreshScheduler()) {
			scheduler->NotifyOnCommit(client);
		}
	}

	// Make the feature own the view so dropping the feature cascades to the view.
	// Version tables are managed explicitly (not via ownership) to allow GC during refresh.
	auto feature_entry = set.GetEntry(transaction, info->feature_name);
	auto &duck_catalog = catalog.Cast<DuckCatalog>();
	duck_catalog.GetDependencyManager()->AddOwnership(transaction, *feature_entry, *view_entry);

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value(info->feature_name));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
