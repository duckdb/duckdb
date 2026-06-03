#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/sql_identifier.hpp"

namespace duckdb {

struct CurrentFeatureBindData : public FunctionData {
	string feature_name;
	vector<string> result_names;
	vector<LogicalType> result_types;
	string default_catalog;
	string default_schema;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<CurrentFeatureBindData>();
		result->feature_name = feature_name;
		result->result_names = result_names;
		result->result_types = result_types;
		result->default_catalog = default_catalog;
		result->default_schema = default_schema;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<CurrentFeatureBindData>();
		return feature_name == o.feature_name;
	}
};

struct CurrentFeatureState : public GlobalTableFunctionState {
	bool done = false;
	unique_ptr<QueryResult> query_result;
};

static unique_ptr<FunctionData> CurrentFeatureBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CurrentFeatureBindData>();
	result->feature_name = input.inputs[0].GetValue<string>();

	// Look up the feature to validate it exists and get current version for schema resolution
	optional_ptr<FeatureCatalogEntry> feature_entry;
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   result->feature_name);
		if (entry) {
			feature_entry = &entry->Cast<FeatureCatalogEntry>();
			break;
		}
	}

	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", result->feature_name);
	}

	// Use current_version to determine schema (columns never change across versions)
	auto versioned_table = result->feature_name + "__v" + duckdb::to_string(feature_entry->current_version);

	// Capture caller's default catalog/schema
	auto &search_path = ClientData::Get(context).catalog_search_path;
	auto &default_entry = search_path->GetDefault();
	result->default_catalog = default_entry.catalog;
	result->default_schema = default_entry.schema;

	// Determine result schema by preparing a query against the current version table
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
	if (!result->default_catalog.empty()) {
		con.Query("USE " + SQLIdentifier::ToString(result->default_catalog));
	}
	if (!result->default_schema.empty() && result->default_schema != DEFAULT_SCHEMA) {
		con.Query("SET schema = '" + result->default_schema + "'");
	}

	auto schema_sql = "SELECT * FROM " + SQLIdentifier::ToString(versioned_table);
	auto prep = con.Prepare(schema_sql);
	if (prep->HasError()) {
		throw CatalogException("Failed to resolve schema for feature \"%s\" (table \"%s\" not found)",
		                       result->feature_name, versioned_table);
	}

	names = prep->GetNames();
	return_types = prep->GetTypes();
	result->result_names = names;
	result->result_types = return_types;

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CurrentFeatureInit(ClientContext &context,
                                                               TableFunctionInitInput &input) {
	return make_uniq<CurrentFeatureState>();
}

static void CurrentFeatureFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<CurrentFeatureState>();
	auto &bind_data = data_p.bind_data->Cast<CurrentFeatureBindData>();

	if (state.done) {
		return;
	}

	// Resolve current version at execution time (may differ from bind time if a refresh ran)
	if (!state.query_result) {
		optional_ptr<FeatureCatalogEntry> feature_entry;
		auto schemas = Catalog::GetAllSchemas(context);
		for (auto &schema : schemas) {
			auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context),
			                                   CatalogType::FEATURE_ENTRY, bind_data.feature_name);
			if (entry) {
				feature_entry = &entry->Cast<FeatureCatalogEntry>();
				break;
			}
		}

		if (!feature_entry) {
			throw CatalogException("Feature \"%s\" does not exist", bind_data.feature_name);
		}

		auto versioned_table =
		    bind_data.feature_name + "__v" + duckdb::to_string(feature_entry->current_version);
		auto query_sql = "SELECT * FROM " + SQLIdentifier::ToString(versioned_table);

		auto &db = DatabaseInstance::GetDatabase(context);
		Connection con(db);
		if (!bind_data.default_catalog.empty()) {
			con.Query("USE " + SQLIdentifier::ToString(bind_data.default_catalog));
		}
		if (!bind_data.default_schema.empty() && bind_data.default_schema != DEFAULT_SCHEMA) {
			con.Query("SET schema = '" + bind_data.default_schema + "'");
		}
		state.query_result = con.Query(query_sql);
		if (state.query_result->HasError()) {
			throw InternalException("current_feature query failed: %s", state.query_result->GetError());
		}
	}

	// Fetch next chunk from the query result
	auto chunk = state.query_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		state.done = true;
		return;
	}

	output.Move(*chunk);
}

void CurrentFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("current_feature", {LogicalType::VARCHAR}, CurrentFeatureFunction,
	                              CurrentFeatureBind, CurrentFeatureInit));
}

} // namespace duckdb
