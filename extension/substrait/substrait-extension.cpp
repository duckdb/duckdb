
#include "substrait-extension.hpp"
#include "to_substrait.hpp"
#include "from_substrait.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() {
	}
	string query;
	bool finished = false;
};

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, vector<Value> &inputs,
                                                named_parameter_map_t &named_parameters,
                                                vector<LogicalType> &input_table_types,
                                                vector<string> &input_table_names, vector<LogicalType> &return_types,
                                                vector<string> &names) {
	auto result = make_unique<ToSubstraitFunctionData>();
	result->query = inputs[0].ToString();
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	return move(result);
}

static void ToSubFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                          DataChunk *input, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*bind_data;
	if (data.finished) {
		return;
	}
	output.SetCardinality(1);
	DuckDBToSubstrait transformer_d2s;
	auto new_conn = Connection(*context.db);
	auto query_plan = new_conn.context->ExtractPlan(data.query);
	transformer_d2s.TransformPlan(*query_plan);
	string serialized;
	transformer_d2s.SerializeToString(serialized);

	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
	data.finished = true;
}

struct FromSubstraitFunctionData : public TableFunctionData {
	FromSubstraitFunctionData() {
	}
	shared_ptr<Relation> plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> conn;
};

static unique_ptr<FunctionData> FromSubstraitBind(ClientContext &context, vector<Value> &inputs,
                                                  named_parameter_map_t &named_parameters,
                                                  vector<LogicalType> &input_table_types,
                                                  vector<string> &input_table_names, vector<LogicalType> &return_types,
                                                  vector<string> &names) {
	auto result = make_unique<FromSubstraitFunctionData>();
	substrait::Plan plan;
	result->conn = make_unique<Connection>(*context.db);
	string serialized = inputs[0].GetValueUnsafe<string>();
	plan.ParseFromString(serialized);
	SubstraitToDuckDB transformer_s2d(*result->conn, plan);
	result->plan = transformer_s2d.TransformPlan(plan);
	for (auto &column : result->plan->Columns()) {
		return_types.emplace_back(column.type);
		names.emplace_back(column.name);
	}
	return move(result);
}

static void FromSubFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                            DataChunk *input, DataChunk &output) {
	auto &data = (FromSubstraitFunctionData &)*bind_data;
	if (!data.res) {
		data.res = data.plan->Execute();
	}
	auto result_chunk = data.res->Fetch();
	if (!result_chunk) {
		return;
	}
	result_chunk->Copy(output);
	//	output.Copy(*result_chunk);
}

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait binary from a valid SQL Query
	TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
	CreateTableFunctionInfo to_sub_info(to_sub_func);
	catalog.CreateTableFunction(*con.context, &to_sub_info);

	// create the from_substrait table function that allows us to get a query result from a substrait plan
	TableFunction from_sub_func("from_substrait", {LogicalType::BLOB}, FromSubFunction, FromSubstraitBind);
	CreateTableFunctionInfo from_sub_info(from_sub_func);
	catalog.CreateTableFunction(*con.context, &from_sub_info);

	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}