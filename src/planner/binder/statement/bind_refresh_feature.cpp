#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/statement/refresh_feature_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_refresh_feature.hpp"

namespace duckdb {

BoundStatement Binder::Bind(RefreshFeatureStatement &stmt) {
	BoundStatement result;
	result.names.emplace_back("rows_affected");
	result.types.emplace_back(LogicalType::BIGINT);
	result.plan = make_uniq<LogicalRefreshFeature>(stmt.feature_name);

	optional_ptr<FeatureCatalogEntry> feature_entry;
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   stmt.feature_name);
		if (entry) {
			feature_entry = &entry->Cast<FeatureCatalogEntry>();
			break;
		}
	}
	if (feature_entry && !feature_entry->temporary) {
		auto modification = DatabaseModificationType::CREATE_CATALOG_ENTRY |
		                    DatabaseModificationType::DROP_CATALOG_ENTRY | DatabaseModificationType::ALTER_TABLE;
		GetStatementProperties().RegisterDBModify(feature_entry->ParentCatalog(), context, modification);
	}

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
