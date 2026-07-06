#include "duckdb/common/feature_serve.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

namespace duckdb {

static optional_ptr<FeatureCatalogEntry> LookupFeature(ClientContext &context, const string &feature_name) {
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   feature_name);
		if (entry) {
			return &entry->Cast<FeatureCatalogEntry>();
		}
	}
	return nullptr;
}

//! Resolve a feature for serving, raising a clear error if it exists but has never been refreshed
//! (current_version == 0, so no version table has been materialized yet).
static FeatureCatalogEntry &ResolveServableFeature(ClientContext &context, const string &feature_name) {
	auto feature_entry = LookupFeature(context, feature_name);
	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", feature_name);
	}
	if (feature_entry->current_version < 1) {
		throw CatalogException("Feature \"%s\" has not been refreshed yet — run REFRESH FEATURE %s first", feature_name,
		                       feature_name);
	}
	return *feature_entry;
}

static unique_ptr<BaseTableRef> BaseTable(const string &table_name, const string &alias) {
	auto result = make_uniq<BaseTableRef>();
	result->table_name = table_name;
	result->alias = alias;
	return result;
}

static unique_ptr<ColumnRefExpression> ColumnRef(const string &alias, const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name, alias);
}

static unique_ptr<ParsedExpression> Conjoin(unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right) {
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(left), std::move(right));
}

static bool ContainsColumn(const vector<string> &columns, const string &column) {
	for (auto &entry : columns) {
		if (entry == column) {
			return true;
		}
	}
	return false;
}

static vector<FeatureServeEntityMapping>
ResolveEntityMappings(const FeatureCatalogEntry &feat, const vector<FeatureServeEntityMapping> &feature_mappings,
                      const string &spine_entity_override) {
	if (feature_mappings.empty() && spine_entity_override.empty()) {
		vector<FeatureServeEntityMapping> result;
		result.reserve(feat.entity_columns.size());
		for (auto &feature_entity : feat.entity_columns) {
			result.push_back(FeatureServeEntityMapping {feature_entity, feature_entity});
		}
		return result;
	}

	if (feat.entity_columns.empty()) {
		throw BinderException("SERVE FEATURE entity mapping was provided for global feature \"%s\"", feat.name);
	}

	if (!spine_entity_override.empty()) {
		if (!feature_mappings.empty()) {
			throw BinderException("SERVE FEATURE cannot combine feature-specific ENTITY mappings with a global ENTITY "
			                      "override");
		}
		if (feat.entity_columns.size() > 1) {
			throw BinderException("SERVE FEATURE with global ENTITY override does not support feature \"%s\" with "
			                      "multiple entity columns",
			                      feat.name);
		}
		return vector<FeatureServeEntityMapping> {
		    FeatureServeEntityMapping {feat.entity_columns[0], spine_entity_override}};
	}

	if (feature_mappings.size() == 1 && feature_mappings[0].feature_column.empty()) {
		if (feat.entity_columns.size() > 1) {
			throw BinderException(
			    "SERVE FEATURE shorthand ENTITY mapping does not support feature \"%s\" with multiple "
			    "entity columns",
			    feat.name);
		}
		return vector<FeatureServeEntityMapping> {
		    FeatureServeEntityMapping {feat.entity_columns[0], feature_mappings[0].spine_column}};
	}

	vector<FeatureServeEntityMapping> result;
	result.reserve(feat.entity_columns.size());
	for (auto &feature_entity : feat.entity_columns) {
		result.push_back(FeatureServeEntityMapping {feature_entity, feature_entity});
	}
	for (auto &mapping : feature_mappings) {
		if (!ContainsColumn(feat.entity_columns, mapping.feature_column)) {
			throw BinderException("Feature \"%s\" has no entity column \"%s\"", feat.name, mapping.feature_column);
		}
		for (auto &resolved : result) {
			if (resolved.feature_column == mapping.feature_column) {
				resolved.spine_column = mapping.spine_column;
				break;
			}
		}
	}
	return result;
}

static unique_ptr<ParsedExpression> ServeJoinCondition(const string &feature_alias,
                                                       const vector<FeatureServeEntityMapping> &entity_mappings,
                                                       const string &spine_ts) {
	unique_ptr<ParsedExpression> condition;
	for (auto &mapping : entity_mappings) {
		auto entity_condition =
		    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, ColumnRef("spine", mapping.spine_column),
		                                    ColumnRef(feature_alias, mapping.feature_column));
		condition =
		    condition ? Conjoin(std::move(condition), std::move(entity_condition)) : std::move(entity_condition);
	}
	// ASOF inequality: for each spine row pick the entity's most recent snapshot at or before the spine
	// timestamp, i.e. the greatest __feature_timestamp that does not exceed the spine's as-of time.
	auto timestamp_condition =
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, ColumnRef("spine", spine_ts),
	                                    ColumnRef(feature_alias, FEATURE_TIMESTAMP_COLUMN));
	return condition ? Conjoin(std::move(condition), std::move(timestamp_condition)) : std::move(timestamp_condition);
}

static unique_ptr<StarExpression> FeatureStar(const string &feature_alias, const vector<string> &feature_entities) {
	auto result = make_uniq<StarExpression>(feature_alias);
	for (auto &feature_entity : feature_entities) {
		result->exclude_list.insert(QualifiedColumnName(feature_entity));
	}
	result->exclude_list.insert(QualifiedColumnName(FEATURE_VERSION_COLUMN));
	result->exclude_list.insert(QualifiedColumnName(FEATURE_TIMESTAMP_COLUMN));
	return result;
}

static void AttachServeJoin(unique_ptr<TableRef> &from_table, const FeatureCatalogEntry &feat,
                            const string &feature_alias, const vector<FeatureServeEntityMapping> &feature_mappings,
                            const string &spine_entity_override, const string &as_of_override) {
	// Serve from the denormalized store table via an ASOF join: every retained version is present, and the
	// join resolves each spine row to the entity's latest snapshot at or before the spine's as-of time.
	auto store_table = FeatureStoreTableName(feat.name);
	auto spine_ts = as_of_override.empty() ? feat.timestamp_column : as_of_override;
	auto entity_mappings = ResolveEntityMappings(feat, feature_mappings, spine_entity_override);

	auto join = make_uniq<JoinRef>(JoinRefType::ASOF);
	join->type = JoinType::LEFT;
	join->left = std::move(from_table);
	join->right = BaseTable(store_table, feature_alias);
	join->condition = ServeJoinCondition(feature_alias, entity_mappings, spine_ts);
	from_table = std::move(join);
}

unique_ptr<SelectStatement> BuildServeFeatureSelect(ClientContext &context, const vector<ServeFeatureRequest> &features,
                                                    const string &spine_table, const string &spine_entity_override,
                                                    const string &spine_asof_column) {
	auto schemas = Catalog::GetAllSchemas(context);
	bool spine_found = false;
	for (auto &schema : schemas) {
		auto entry =
		    schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::TABLE_ENTRY, spine_table);
		if (entry) {
			spine_found = true;
			break;
		}
	}
	if (!spine_found) {
		throw CatalogException("Spine table \"%s\" does not exist", spine_table);
	}

	if (features.size() == 1) {
		auto &request = features[0];
		auto &feat = ResolveServableFeature(context, request.feature_name);

		auto select = make_uniq<SelectNode>();
		select->select_list.push_back(make_uniq<StarExpression>("spine"));
		select->select_list.push_back(FeatureStar("f", feat.entity_columns));
		select->from_table = BaseTable(spine_table, "spine");
		AttachServeJoin(select->from_table, feat, "f", request.entity_mappings, spine_entity_override,
		                spine_asof_column);

		auto result = make_uniq<SelectStatement>();
		result->node = std::move(select);
		return result;
	}

	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>("spine"));
	select->from_table = BaseTable(spine_table, "spine");

	for (idx_t i = 0; i < features.size(); i++) {
		auto &request = features[i];
		auto &feat = ResolveServableFeature(context, request.feature_name);
		auto alias = "f" + duckdb::to_string(i);

		select->select_list.push_back(FeatureStar(alias, feat.entity_columns));
		AttachServeJoin(select->from_table, feat, alias, request.entity_mappings, spine_entity_override,
		                spine_asof_column);
	}

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(select);
	return result;
}

} // namespace duckdb
