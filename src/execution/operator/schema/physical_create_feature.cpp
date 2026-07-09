#include "duckdb/execution/operator/schema/physical_create_feature.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/feature_refresh_scheduler.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

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
			FlatVector::SetSize(chunk.data[0], 1);
			chunk.SetCardinality(1);
			chunk.data[0].SetValue(0, Value(info->feature_name));
			return SourceResultType::FINISHED;
		}
	}

	// CREATE FEATURE only registers metadata; no store table is materialized here. The first REFRESH FEATURE
	// creates the denormalized store (feature_name__store). Until then current_version is 0, and
	// queries/serves against the feature raise an error asking the user to refresh it first. A feature name
	// used as a relation ("FROM my_feature") resolves to its current-version snapshot directly in the binder,
	// so no shadow view is created; the store table is dropped explicitly on DROP FEATURE.
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

	FlatVector::SetSize(chunk.data[0], 1);
	chunk.SetCardinality(1);
	chunk.data[0].SetValue(0, Value(info->feature_name));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
