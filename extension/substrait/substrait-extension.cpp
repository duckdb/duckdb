
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

void CompareQueryResults(QueryResult &actual_result, QueryResult &roundtrip_result) {
	// actual_result compare the success state of the results
	if (!actual_result.success) {
		throw InternalException("Query failed");
	}
	if (actual_result.success != roundtrip_result.success) {
		throw InternalException("Roundtrip substrait plan failed");
	}

	// FIXME: How to name expression?
	//	// compare names
	//	if (names != other.names) {
	//		return false;
	//	}
	// compare types
	if (actual_result.types != roundtrip_result.types) {
		throw InternalException("Substrait Plan Types differ from Actual Result Types");
	}
	// now compare the actual values
	// fetch chunks
	while (true) {
		auto lchunk = actual_result.Fetch();
		auto rchunk = roundtrip_result.Fetch();
		if (!lchunk && !rchunk) {
			return;
		}
		if (!lchunk || !rchunk) {
			throw InternalException("Substrait Plan Types chunk differs from Actual Result chunk");
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return;
		}
		if (lchunk->size() != rchunk->size()) {
			throw InternalException("Substrait Plan Types chunk size differs from Actual Result chunk size");
		}
		for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
			for (idx_t row = 0; row < rchunk->size(); row++) {
				auto lvalue = lchunk->GetValue(col, row);
				auto rvalue = rchunk->GetValue(col, row);
				if (lvalue.IsNull() && rvalue.IsNull()) {
					continue;
				}
				if (lvalue != rvalue) {
					lchunk->Print();
					rchunk->Print();
					throw InternalException("Substrait Plan Result differs from Actual Result");
				}
			}
		}
	}
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
		CompareQueryResults(*actual_result, *substrait_result);
	}
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
	result->conn = make_unique<Connection>(*context.db);
	string serialized = inputs[0].GetValueUnsafe<string>();
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