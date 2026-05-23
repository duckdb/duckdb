#include "duckdb/execution/operator/schema/physical_create_feature.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->catalog);
	auto &schema = catalog.GetSchema(context.client, info->schema);
	auto transaction = catalog.GetCatalogTransaction(context.client);
	auto entry = make_uniq<FeatureCatalogEntry>(catalog, schema, *info);
	auto entry_name = entry->name;
	auto &set = schema.Cast<DuckSchemaEntry>().GetCatalogSet(CatalogType::FEATURE_ENTRY);
	LogicalDependencyList dependencies;
	if (info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto old_entry = set.GetEntry(transaction, entry_name);
		if (old_entry) {
			return SourceResultType::FINISHED;
		}
	}
	if (!set.CreateEntry(transaction, entry_name, std::move(entry), dependencies)) {
		throw CatalogException::EntryAlreadyExists(CatalogType::FEATURE_ENTRY, entry_name);
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
