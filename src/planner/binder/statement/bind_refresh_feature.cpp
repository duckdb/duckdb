#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/feature_refresh.hpp"
#include "duckdb/common/types/timestamp.hpp"
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

	// The snapshot timestamp: the AT clause when given, otherwise the current time.
	timestamp_t feature_ts = stmt.at_timestamp.empty() ? Timestamp::GetCurrentTimestamp()
	                                                    : Timestamp::FromString(stmt.at_timestamp, false);

	// Build and bind the query that produces the full contents of the next denormalized store table: the
	// new snapshot UNION ALL the still-retained rows from the previous store. Its result schema defines the
	// new store table; the plan becomes the child of the refresh operator.
	auto refresh_query = BuildFeatureRefreshQuery(feat, feature_ts, feat.current_version + 1);
	auto query_binder = Binder::CreateBinder(context, this);
	auto query_obj = query_binder->Bind(*refresh_query);
	D_ASSERT(query_obj.names.size() >= 1);

	auto refresh_node = make_uniq<LogicalRefreshFeature>(stmt.feature_name);
	refresh_node->result_names = query_obj.names;
	refresh_node->result_types = query_obj.types;
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
