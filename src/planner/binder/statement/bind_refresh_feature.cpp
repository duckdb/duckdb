#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/feature_refresh.hpp"
#include "duckdb/parser/statement/refresh_feature_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_refresh_feature.hpp"

namespace duckdb {

BoundStatement Binder::Bind(RefreshFeatureStatement &stmt) {
	BoundStatement result;
	result.names.emplace_back("rows_affected");
	result.types.emplace_back(LogicalType::BIGINT);

	// Look up the feature. This uses the current transaction, so a feature created earlier in the same
	// (uncommitted) transaction is visible.
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
	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", stmt.feature_name);
	}
	auto &feat = *feature_entry;

	// Build and bind the query that produces the full contents of the next feature version. Its result
	// schema defines the new version table; the plan becomes the child of the refresh operator. The
	// query projects a trailing boolean marker column (see BuildFeatureRefreshQuery) that flags the
	// recomputed rows — the operator sums it for rows_affected and strips it before appending, so it is
	// excluded from the version-table schema here.
	auto refresh_query = BuildFeatureRefreshQuery(feat);
	auto query_binder = Binder::CreateBinder(context, this);
	auto query_obj = query_binder->Bind(*refresh_query);
	D_ASSERT(query_obj.names.size() >= 2);

	auto refresh_node = make_uniq<LogicalRefreshFeature>(stmt.feature_name);
	refresh_node->result_names.assign(query_obj.names.begin(), query_obj.names.end() - 1);
	refresh_node->result_types.assign(query_obj.types.begin(), query_obj.types.end() - 1);
	refresh_node->children.push_back(std::move(query_obj.plan));
	result.plan = std::move(refresh_node);

	if (!feat.temporary) {
		auto modification = DatabaseModificationType::CREATE_CATALOG_ENTRY |
		                    DatabaseModificationType::DROP_CATALOG_ENTRY | DatabaseModificationType::ALTER_TABLE;
		GetStatementProperties().RegisterDBModify(feat.ParentCatalog(), context, modification);
	}

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
