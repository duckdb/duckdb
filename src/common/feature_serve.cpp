#include "duckdb/common/feature_serve.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
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

static unique_ptr<BaseTableRef> BaseTable(const string &table_name, const string &alias) {
	auto result = make_uniq<BaseTableRef>();
	result->table_name = table_name;
	result->alias = alias;
	return result;
}

static unique_ptr<ColumnRefExpression> ColumnRef(const string &alias, const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name, alias);
}

static unique_ptr<ParsedExpression> ServeJoinCondition(const string &feature_alias, const string &feature_entity,
                                                       const string &spine_entity, const string &spine_ts) {
	auto entity_condition = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, ColumnRef("spine", spine_entity),
	                                                       ColumnRef(feature_alias, feature_entity));
	auto timestamp_condition = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                                          ColumnRef("spine", spine_ts),
	                                                          ColumnRef(feature_alias, "feature_timestamp"));
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(entity_condition),
	                                        std::move(timestamp_condition));
}

static unique_ptr<StarExpression> FeatureStar(const string &feature_alias, const string &feature_entity) {
	auto result = make_uniq<StarExpression>(feature_alias);
	result->exclude_list.insert(QualifiedColumnName(feature_entity));
	result->exclude_list.insert(QualifiedColumnName("feature_timestamp"));
	return result;
}

static void AddServeJoin(unique_ptr<TableRef> &from_table, const FeatureCatalogEntry &feat, const string &feature_alias,
                         const string &entity_override, const string &as_of_override) {
	auto versioned_table = feat.name + "__v" + duckdb::to_string(feat.current_version);
	auto feat_entity = feat.entity_column;
	auto spine_entity = entity_override.empty() ? feat_entity : entity_override;
	auto spine_ts = as_of_override.empty() ? feat.timestamp_column : as_of_override;

	auto join = make_uniq<JoinRef>(JoinRefType::ASOF);
	join->type = JoinType::LEFT;
	join->left = std::move(from_table);
	join->right = BaseTable(versioned_table, feature_alias);
	join->condition = ServeJoinCondition(feature_alias, feat_entity, spine_entity, spine_ts);
	from_table = std::move(join);
}

unique_ptr<SelectStatement> BuildServeFeatureSelect(ClientContext &context, const vector<string> &feature_list,
                                                    const string &spine_table, const string &entity_override,
                                                    const string &as_of_override) {
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

	if (feature_list.size() > 1 && entity_override.empty()) {
		string first_entity;
		for (auto &fname : feature_list) {
			auto entry = LookupFeature(context, fname);
			if (!entry) {
				throw CatalogException("Feature \"%s\" does not exist", fname);
			}
			if (first_entity.empty()) {
				first_entity = entry->entity_column;
			} else if (entry->entity_column != first_entity) {
				throw BinderException("Features have different entity columns (\"%s\" vs \"%s\"). "
				                      "Use ENTITY to specify the spine's entity column explicitly.",
				                      first_entity, entry->entity_column);
			}
		}
	}

	if (feature_list.size() == 1) {
		auto feature_entry = LookupFeature(context, feature_list[0]);
		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", feature_list[0]);
		}
		auto &feat = *feature_entry;

		auto select = make_uniq<SelectNode>();
		select->select_list.push_back(make_uniq<StarExpression>("spine"));
		select->select_list.push_back(ColumnRef("f", "feature_timestamp"));
		select->select_list.push_back(FeatureStar("f", feat.entity_column));
		select->from_table = BaseTable(spine_table, "spine");
		AddServeJoin(select->from_table, feat, "f", entity_override, as_of_override);

		auto result = make_uniq<SelectStatement>();
		result->node = std::move(select);
		return result;
	}

	auto select = make_uniq<SelectNode>();
	select->select_list.push_back(make_uniq<StarExpression>("spine"));
	select->from_table = BaseTable(spine_table, "spine");

	for (idx_t i = 0; i < feature_list.size(); i++) {
		auto feature_entry = LookupFeature(context, feature_list[i]);
		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", feature_list[i]);
		}
		auto &feat = *feature_entry;
		auto alias = "f" + duckdb::to_string(i);

		auto timestamp = ColumnRef(alias, "feature_timestamp");
		timestamp->SetAlias(feat.name + "_timestamp");
		select->select_list.push_back(std::move(timestamp));
		select->select_list.push_back(FeatureStar(alias, feat.entity_column));
		AddServeJoin(select->from_table, feat, alias, entity_override, as_of_override);
	}

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(select);
	return result;
}

} // namespace duckdb
