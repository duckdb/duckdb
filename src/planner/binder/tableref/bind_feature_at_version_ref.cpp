#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/tableref/feature_at_version_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(FeatureAtVersionRef &ref) {
	QueryErrorContext error_context(ref.query_location);

	string catalog_name;
	string schema_name;
	BindSchemaOrCatalog(entry_retriever, catalog_name, schema_name);
	EntryLookupInfo feature_lookup(CatalogType::FEATURE_ENTRY, ref.feature_name, error_context);
	auto feature_or_null =
	    entry_retriever.GetEntry(catalog_name, schema_name, feature_lookup, OnEntryNotFound::RETURN_NULL);
	if (!feature_or_null) {
		throw CatalogException("Feature \"%s\" does not exist", ref.feature_name);
	}
	auto &feature = feature_or_null->Cast<FeatureCatalogEntry>();
	if (feature.current_version < 1) {
		throw CatalogException("Feature \"%s\" has not been refreshed yet — run REFRESH FEATURE %s first", feature.name,
		                       feature.name);
	}

	auto feature_ref =
	    BuildFeatureVersionRef(feature.ParentCatalog().GetName(), feature.ParentSchema().name, feature.name,
	                           ref.version, ref.alias.empty() ? feature.name : ref.alias, ref.column_name_alias);
	return Bind(*feature_ref);
}

} // namespace duckdb
