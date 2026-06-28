#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/feature_serve.hpp"
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

struct ServeFeatureBindData : public FunctionData {
	string feature_names; // comma-separated
	string spine_table;
	string entity_column; // empty = use feature's entity column name (assumes spine has same name)
	string as_of_column;  // empty = use feature's timestamp column name (assumes spine has same name)
	vector<string> result_names;
	vector<LogicalType> result_types;
	string generated_sql;
	string default_catalog; // caller's default catalog for sub-connections
	string default_schema;  // caller's default schema for sub-connections

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ServeFeatureBindData>();
		result->feature_names = feature_names;
		result->spine_table = spine_table;
		result->entity_column = entity_column;
		result->as_of_column = as_of_column;
		result->result_names = result_names;
		result->result_types = result_types;
		result->generated_sql = generated_sql;
		result->default_catalog = default_catalog;
		result->default_schema = default_schema;
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

static void SetConnectionCatalog(Connection &con, const string &catalog, const string &schema) {
	if (!catalog.empty()) {
		auto use_result = con.Query("USE " + QuoteId(catalog));
		if (use_result->HasError()) {
			throw InternalException("Failed to set catalog for SERVE FEATURE: %s", use_result->GetError());
		}
	}
	if (!schema.empty() && schema != DEFAULT_SCHEMA) {
		auto schema_result = con.Query("SET schema = '" + schema + "'");
		if (schema_result->HasError()) {
			throw InternalException("Failed to set schema for SERVE FEATURE: %s", schema_result->GetError());
		}
	}
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

	// Capture the caller's default catalog/schema so sub-connections resolve tables correctly
	auto &search_path = ClientData::Get(context).catalog_search_path;
	auto &default_entry = search_path->GetDefault();
	result->default_catalog = default_entry.catalog;
	result->default_schema = default_entry.schema;

	// Build the SQL — empty entity/as_of means "use same column name as feature metadata"
	result->generated_sql =
	    BuildServeFeatureSQL(context, feature_list, result->spine_table, result->entity_column, result->as_of_column);

	// Use a sub-connection to determine the result schema
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
	SetConnectionCatalog(con, result->default_catalog, result->default_schema);
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
		SetConnectionCatalog(con, bind_data.default_catalog, bind_data.default_schema);
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
