#define DUCKDB_EXTENSION_MAIN

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

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	return move(result);
}

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, string &serialized) {
	SubstraitToDuckDB transformer_s2d(conn, serialized);
	return transformer_s2d.TransformPlan();
}

static void ToSubFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                          DataChunk *input, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*bind_data;
	if (data.finished) {
		return;
	}
	output.SetCardinality(1);
	auto new_conn = Connection(*context.db);
	auto query_plan = new_conn.context->ExtractPlan(data.query);
	DuckDBToSubstrait transformer_d2s(*query_plan);
	auto serialized = transformer_d2s.SerializeToString();

	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
	data.finished = true;
	if (context.config.query_verification_enabled) {
		// We round-trip the generated blob and verify if the result is the same
		auto actual_result = new_conn.Query(data.query);
		auto sub_relation = SubstraitPlanToDuckDBRel(new_conn, serialized);
		auto substrait_result = sub_relation->Execute();
		if (!actual_result->Equals(*substrait_result)) {
			query_plan->Print();
			sub_relation->Print();
			throw InternalException("The query result of DuckDB's query plan does not match Substrait");
		}
	}
}

struct FromSubstraitFunctionData : public TableFunctionData {
	FromSubstraitFunctionData() {
	}
	shared_ptr<Relation> plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> conn;
};

static unique_ptr<FunctionData> FromSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<FromSubstraitFunctionData>();
	result->conn = make_unique<Connection>(*context.db);
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	result->plan = SubstraitPlanToDuckDBRel(*result->conn, serialized);
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
	// Move should work here, no?
	result_chunk->Copy(output);
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