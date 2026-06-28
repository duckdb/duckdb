#include "duckdb/common/feature_serve.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

static string QuoteId(const string &name) {
	return SQLIdentifier::ToString(name);
}

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

string BuildServeFeatureSQL(ClientContext &context, const vector<string> &feature_list, const string &spine_table,
                            const string &entity_override, const string &as_of_override) {
	// For each feature, build an ASOF JOIN against the spine
	// Only uses the current version from the feature metadata
	// entity_override: if non-empty, use this as the spine's entity column (maps to feature's entity)
	// as_of_override: if non-empty, use this as the spine's timestamp column
	// If empty, assume spine has same column name as the feature's entity/timestamp column

	// Validate that spine table exists
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

	// For multiple features without an entity override, validate they all share the same entity column
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
		// Single feature: simple ASOF JOIN
		auto feature_entry = LookupFeature(context, feature_list[0]);
		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", feature_list[0]);
		}
		auto &feat = *feature_entry;
		auto versioned_table = feat.name + "__v" + duckdb::to_string(feat.current_version);
		auto feat_table = QuoteId(versioned_table);
		auto feat_entity = QuoteId(feat.entity_column);
		// Spine column names: use override if provided, else same as feature's column name
		auto spine_entity = entity_override.empty() ? feat_entity : QuoteId(entity_override);
		auto spine_ts = as_of_override.empty() ? QuoteId(feat.timestamp_column) : QuoteId(as_of_override);
		auto spine = QuoteId(spine_table);

		return StringUtil::Format("SELECT spine.*, f.feature_timestamp, "
		                          "f.* EXCLUDE (%s, feature_timestamp) "
		                          "FROM %s AS spine "
		                          "ASOF LEFT JOIN %s AS f "
		                          "ON spine.%s = f.%s AND spine.%s >= f.feature_timestamp",
		                          feat_entity,               // EXCLUDE
		                          spine,                     // FROM
		                          feat_table,                // feature version table
		                          spine_entity, feat_entity, // entity join (spine col = feature col)
		                          spine_ts                   // temporal condition
		);
	}

	// Multiple features: chain of ASOF JOINs
	string sql = "SELECT spine.*";
	string joins;

	for (idx_t i = 0; i < feature_list.size(); i++) {
		auto feature_entry = LookupFeature(context, feature_list[i]);
		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", feature_list[i]);
		}
		auto &feat = *feature_entry;
		auto versioned_table = feat.name + "__v" + duckdb::to_string(feat.current_version);
		auto feat_table = QuoteId(versioned_table);
		auto feat_entity = QuoteId(feat.entity_column);
		auto spine_entity = entity_override.empty() ? feat_entity : QuoteId(entity_override);
		auto spine_ts = as_of_override.empty() ? QuoteId(feat.timestamp_column) : QuoteId(as_of_override);
		auto alias = "f" + duckdb::to_string(i);

		sql += StringUtil::Format(", %s.feature_timestamp AS %s_timestamp, %s.* EXCLUDE (%s, feature_timestamp)", alias,
		                          feat.name, alias, feat_entity);

		joins += StringUtil::Format(" ASOF LEFT JOIN %s AS %s "
		                            "ON spine.%s = %s.%s AND spine.%s >= %s.feature_timestamp",
		                            feat_table, alias, spine_entity, alias, feat_entity, spine_ts, alias);
	}

	sql += " FROM " + QuoteId(spine_table) + " AS spine" + joins;
	return sql;
}

} // namespace duckdb