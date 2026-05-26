#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

struct ServeFeatureBindData : public FunctionData {
	string feature_names; // comma-separated
	string spine_table;
	string entity_column; // empty = use feature's entity column name (assumes spine has same name)
	string as_of_column;  // empty = use feature's timestamp column name (assumes spine has same name)
	vector<string> result_names;
	vector<LogicalType> result_types;
	string generated_sql;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ServeFeatureBindData>();
		result->feature_names = feature_names;
		result->spine_table = spine_table;
		result->entity_column = entity_column;
		result->as_of_column = as_of_column;
		result->result_names = result_names;
		result->result_types = result_types;
		result->generated_sql = generated_sql;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<ServeFeatureBindData>();
		return feature_names == o.feature_names && spine_table == o.spine_table && entity_column == o.entity_column &&
		       as_of_column == o.as_of_column;
	}
};

struct ServeFeatureState : public GlobalTableFunctionState {
	bool done = false;
	unique_ptr<QueryResult> query_result;
};

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

static string BuildServeSQL(ClientContext &context, const vector<string> &feature_list, const string &spine_table,
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
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::TABLE_ENTRY,
		                                   spine_table);
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
				throw BinderException(
				    "Features have different entity columns (\"%s\" vs \"%s\"). "
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
		auto feat_table = QuoteId(feat.name);
		auto feat_entity = QuoteId(feat.entity_column);
		// Spine column names: use override if provided, else same as feature's column name
		auto spine_entity = entity_override.empty() ? feat_entity : QuoteId(entity_override);
		auto spine_ts = as_of_override.empty() ? QuoteId(feat.timestamp_column) : QuoteId(as_of_override);
		auto spine = QuoteId(spine_table);
		auto version = duckdb::to_string(feat.current_version);

		return StringUtil::Format("SELECT spine.*, f.feature_timestamp, "
		                          "f.* EXCLUDE (%s, feature_timestamp, __feature_version) "
		                          "FROM %s AS spine "
		                          "ASOF JOIN (SELECT * FROM %s WHERE __feature_version = %s) AS f "
		                          "ON spine.%s = f.%s AND spine.%s >= f.feature_timestamp",
		                          feat_entity,               // EXCLUDE
		                          spine,                     // FROM
		                          feat_table, version,       // subquery with version filter
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
		auto feat_table = QuoteId(feat.name);
		auto feat_entity = QuoteId(feat.entity_column);
		auto spine_entity = entity_override.empty() ? feat_entity : QuoteId(entity_override);
		auto spine_ts = as_of_override.empty() ? QuoteId(feat.timestamp_column) : QuoteId(as_of_override);
		auto version = duckdb::to_string(feat.current_version);
		auto alias = "f" + duckdb::to_string(i);

		sql += StringUtil::Format(", %s.feature_timestamp AS %s_timestamp, %s.* EXCLUDE (%s, feature_timestamp, "
		                          "__feature_version)",
		                          alias, feat.name, alias, feat_entity);

		joins += StringUtil::Format(" ASOF JOIN (SELECT * FROM %s WHERE __feature_version = %s) AS %s "
		                            "ON spine.%s = %s.%s AND spine.%s >= %s.feature_timestamp",
		                            feat_table, version, alias, spine_entity, alias, feat_entity, spine_ts, alias);
	}

	sql += " FROM " + QuoteId(spine_table) + " AS spine" + joins;
	return sql;
}

static unique_ptr<FunctionData> ServeFeatureBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ServeFeatureBindData>();
	result->feature_names = input.inputs[0].GetValue<string>();
	result->spine_table = input.inputs[1].GetValue<string>();
	result->entity_column = input.inputs[2].GetValue<string>();
	result->as_of_column = input.inputs[3].GetValue<string>();

	// Parse feature names (comma-separated)
	vector<string> feature_list = StringUtil::Split(result->feature_names, ',');
	for (auto &f : feature_list) {
		StringUtil::Trim(f);
	}

	// Build the SQL — empty entity/as_of means "use same column name as feature metadata"
	result->generated_sql =
	    BuildServeSQL(context, feature_list, result->spine_table, result->entity_column, result->as_of_column);

	// Use a sub-connection to determine the result schema
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
	auto prep = con.Prepare(result->generated_sql);
	if (prep->HasError()) {
		throw BinderException("Failed to prepare SERVE FEATURE query: %s\nGenerated SQL: %s", prep->GetError(),
		                      result->generated_sql);
	}

	names = prep->GetNames();
	return_types = prep->GetTypes();

	result->result_names = names;
	result->result_types = return_types;

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ServeFeatureInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ServeFeatureState>();
}

static void ServeFeatureFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<ServeFeatureState>();
	auto &bind_data = data_p.bind_data->Cast<ServeFeatureBindData>();

	if (state.done) {
		return;
	}

	// Execute the query on first call
	if (!state.query_result) {
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection con(db);
		state.query_result = con.Query(bind_data.generated_sql);
		if (state.query_result->HasError()) {
			throw InternalException("SERVE FEATURE query failed: %s", state.query_result->GetError());
		}
	}

	// Fetch next chunk
	auto chunk = state.query_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		state.done = true;
		return;
	}

	output.Move(*chunk);
}

void ServeFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction(
	    "serve_feature", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    ServeFeatureFunction, ServeFeatureBind, ServeFeatureInit));
}

} // namespace duckdb
