#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/feature_refresh.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/statement/refresh_feature_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_refresh_feature.hpp"

namespace duckdb {

//! True when the feature's entity key columns are provably unique in the entity table, i.e. a PRIMARY KEY (NOT
//! NULL + unique) is fully contained in them. Then the entity spine's DISTINCT removes nothing and can be
//! dropped. Only PRIMARY KEY qualifies -- a nullable UNIQUE constraint could still have several NULL-key rows
//! that DISTINCT collapses, so it is left alone.
static bool EntityKeyIsUnique(const TableCatalogEntry &entity, const vector<string> &key_columns) {
	if (key_columns.empty()) {
		return false;
	}
	auto key_contains = [&](const string &column) {
		for (auto &key : key_columns) {
			if (key == column) {
				return true;
			}
		}
		return false;
	};
	for (auto &constraint : entity.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		vector<string> pk_columns = unique.HasIndex()
		                                ? vector<string> {entity.GetColumns().GetColumn(unique.GetIndex()).Name()}
		                                : unique.GetColumnNames();
		bool covers = true;
		for (auto &pk_column : pk_columns) {
			if (!key_contains(pk_column)) {
				covers = false;
				break;
			}
		}
		if (covers) {
			return true;
		}
	}
	return false;
}

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
	timestamp_t feature_ts =
	    stmt.at_timestamp.empty() ? Timestamp::GetCurrentTimestamp() : Timestamp::FromString(stmt.at_timestamp, false);

	// If the entity table's PRIMARY KEY covers the feature's entity keys, the entity spine is already unique and
	// its DISTINCT (a full hash-aggregate over the entity table on every refresh) can be dropped. Look the entity
	// table up in the feature's own catalog/schema; if it can't be resolved, keep the DISTINCT (correct default).
	bool entity_key_is_unique = false;
	auto entity_entry = feat.ParentCatalog().GetEntry<TableCatalogEntry>(
	    context, feat.ParentSchema().name, feat.entity_table, OnEntryNotFound::RETURN_NULL);
	if (entity_entry) {
		entity_key_is_unique = EntityKeyIsUnique(*entity_entry, feat.entity_key_columns);
	}

	// Build and bind the query that produces the new snapshot (one row per entity at feature_ts, tagged with the
	// new version and timestamp). Its result schema defines the store table; the plan becomes the refresh child.
	auto refresh_query = BuildFeatureRefreshQuery(feat, feature_ts, feat.current_version + 1, entity_key_is_unique);
	auto query_binder = Binder::CreateBinder(context, this);
	auto query_obj = query_binder->Bind(*refresh_query);
	D_ASSERT(query_obj.names.size() >= 1);

	auto refresh_node = make_uniq<LogicalRefreshFeature>(stmt.feature_name);
	refresh_node->result_names = query_obj.names;
	refresh_node->result_types = query_obj.types;
	refresh_node->feature_timestamp = feature_ts;
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
